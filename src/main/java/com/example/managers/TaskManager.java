
package com.example.managers;

import com.sun.management.UnixOperatingSystemMXBean;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.SharedData;
import net.openhft.chronicle.queue.ExcerptAppender;
import org.hyperic.sigar.NetStat;
import org.hyperic.sigar.Sigar;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Random;
import java.util.UUID;


/**
 * simple worker for test.
 */
public class TaskManager extends AbstractVerticle {
    /**
     * logger.
     */
    private static Logger logger = LoggerFactory.getLogger("taskDomains");
    /**
     * metric manager.
     */
    private static MetricsManager mm = new MetricsManager(logger, "taskDomains");
    /**
     * ChronicleQueueManager.
     */
    private ChronicleQueueManager cqm = new ChronicleQueueManager(".");
    /**
     * appender.
     */
    private ExcerptAppender domainResultsAppender = null;
    /**
     * OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
     */
    private OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    /**
     * sharedata.
     */
    private static SharedData sd = null;
    /**
     * localmaps.
     */
    private static LocalMap<String, String> configLocalMap = null;
    /**
     * craweler count.
     */
    private int crawlerCount = 10;
    /**
     * sigar.
     */
    private static Sigar sigar = null;

    @Override
    public void start() {
        try {
            sigar = new Sigar();
            sd = vertx.sharedData();
            configLocalMap = sd.getLocalMap("configLocalMap");
            crawlerCount = Integer.parseInt(configLocalMap.getOrDefault("crawlerCount", "10"));
            cqm.setQueue("domains", "job1");
            cqm.setQueue("domain-results", "job1");
            domainResultsAppender = cqm.getAppender("domain-results");
            setTaskFeed();
            setComputeMetrics();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        cqm.closeQueue("domains");
        cqm.closeQueue("domain-results");
    }

    /**
     * set compute metrics.
     */

    private void setComputeMetrics() {
        vertx.setPeriodic(1000L, h -> {
            try {
                Double openFileDescriptorCount = new Double(((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount());
                NetStat netstat = sigar.getNetStat();
                System.out.println("FD:" + openFileDescriptorCount
                        + " TCP->Out:" + netstat.getAllOutboundTotal()
                        + " SynSent:" + netstat.getTcpSynSent()
                        + " LastAck:" + netstat.getTcpLastAck()
                        + " Est:" + netstat.getTcpEstablished()
                        + " FinWait1:" + netstat.getTcpFinWait1()
                        + " FinWait2:" + netstat.getTcpFinWait2()
                        + " TimeWait:" + netstat.getTcpTimeWait()
                        + " CloseWait:" + netstat.getTcpCloseWait()
                        + " Close:" + netstat.getTcpClose()
                        + " task.working:" + mm.getCounter("task.working").getCount()
                        + " finished:" + mm.getCounter("task.finished").getCount()
                        + " succeeded:" + mm.getCounter("task.succeeded").getCount()
                        + " timeout:" + mm.getCounter("task.timeout").getCount()
                );
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * task
     */
    private void setTaskFeed() {
        String endpoint = UUID.randomUUID().toString();
        for (int i = 0; i < crawlerCount; i++) {
            final String address = endpoint + i;
            vertx.eventBus().<JsonObject>consumer(address, message -> {
                message.reply("");
                final JsonObject taskJo = message.body();
                if (taskJo.isEmpty()) {
                    final String domain = cqm.readText("domains", "job1");
                    if (domain != null) {
                        taskJo.put("url", "http://www." + domain);
                    }
                }
                if (taskJo.isEmpty()) {
                    vertx.setTimer(30000, id -> {
                        System.out.println("queue is empty. waiting for new element......");
                        vertx.eventBus().send(address, new JsonObject());
                    });
                } else {
                    try {
                        mm.getCounter("task.send").inc();
                        mm.getCounter("task.working").inc();
                        vertx.eventBus().<JsonObject>send("taskFeed", taskJo
                                , new DeliveryOptions().setSendTimeout(120000L), res -> {
                                    if (res.succeeded()) {
                                        JsonObject jo = res.result().body();
                                        mm.getCounter("task.working").dec();
                                        mm.getCounter("task.finished").inc();
                                        mm.getMeter("task.finished").mark();
                                        mm.getCounter("task.succeeded").inc();
                                        mm.getMeter("task.succeeded").mark();
                                        domainResultsAppender.writeText(jo.toString());
                                        vertx.eventBus().send(address, new JsonObject());
                                    } else {
                                        mm.getCounter("task.working").dec();
                                        mm.getCounter("task.finished").inc();
                                        mm.getMeter("task.finished").mark();
                                        ReplyException e = (ReplyException) res.cause();
                                        ReplyFailure rf = e.failureType();
                                        switch (rf) {
                                            case RECIPIENT_FAILURE:
                                                mm.getCounter("task.failed").inc();
                                                vertx.setTimer(1000, id -> {
                                                    vertx.eventBus().send(address, taskJo);
                                                });
                                            case TIMEOUT:
                                                mm.getCounter("task.timeout").inc();
                                                mm.getMeter("task.timeout").mark();
                                                int delay = new Random().nextInt(10) * ((int) mm.getMeter("task.timeout").getFiveMinuteRate()) / 10;
                                                vertx.setTimer(delay <= 0 ? 1000 : delay, id -> {
                                                    vertx.eventBus().send(address, taskJo);
                                                });
                                                break;
                                            default:
                                                vertx.setTimer(1000, id -> {
                                                    vertx.eventBus().send(address, taskJo);
                                                });
                                                break;
                                        }
                                    }
                                });
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            vertx.eventBus().send(address, new JsonObject());
        }
    }

}