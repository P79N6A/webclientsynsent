package com.example.managers;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.concurrent.ConcurrentHashMap;

public class ChronicleQueueManager {

    /**
     * queue hash map.
     */
    private static ConcurrentHashMap<String, ChronicleQueue> chronicleQueueConcurrentHashMap = new ConcurrentHashMap<String, ChronicleQueue>();

    /**
     * appender hash map.
     */
    private static ConcurrentHashMap<String, ExcerptAppender> exceptAppenderConcurrentHashMap = new ConcurrentHashMap<String, ExcerptAppender>();

    /**
     * appender hash map.
     */
    private static ConcurrentHashMap<String, ExcerptTailer> exceptTailerConcurrentHashMap = new ConcurrentHashMap<String, ExcerptTailer>();
    /**
     * base dir.
     */

    private String baseDirectory = ".";

    /**
     * init.
     *
     * @param baseDir base dir
     */
    public ChronicleQueueManager(final String baseDir) {
        if (baseDir != null) {
            this.baseDirectory = baseDir;
        }
    }

    /**
     * get queue.
     *
     * @param queueDirectory main directory of queue
     * @param consumerId     consumer id
     */
    public void setQueue(final String queueDirectory, final String consumerId) {
        try {
            if (new File("./cqs/domains").exists()) {
                FileUtils.deleteDirectory(new File("./cqs/domains"));
            }
            if (!new File("./cqs/domains").exists()) {
                try {
                    ChronicleQueue domains = SingleChronicleQueueBuilder
                            .single("./cqs/domains/main").rollCycle(RollCycles.DAILY)
                            .build();
                    ExcerptAppender appenderDomains = domains.acquireAppender();

                    BufferedReader br = null;
                    FileReader fr = null;
                    fr = new FileReader("./domains");
                    br = new BufferedReader(fr);
                    long i = 0L;
                    String strLine;
                    while ((strLine = br.readLine()) != null) {
                        try {
                            strLine = strLine.toLowerCase().trim();
                            appenderDomains.writeText(strLine);
                            i++;
                            if (i % 100000 == 0) {
                                System.out.println(i + ":" + strLine + "-index:" + appenderDomains.lastIndexAppended());
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    br.close();
                    fr.close();
                    domains.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            String mainQueueDirectory = getMainQueueDirectory(queueDirectory);
            System.out.println("mainQueueDirectory:" + mainQueueDirectory);
            ChronicleQueue queue = SingleChronicleQueueBuilder.single(mainQueueDirectory)
                    .rollCycle(RollCycles.DAILY).build();
            chronicleQueueConcurrentHashMap.put(mainQueueDirectory, queue);
            exceptAppenderConcurrentHashMap.put(mainQueueDirectory, queue.acquireAppender());
            ExcerptTailer tailerQueue = queue.createTailer();
            // read position
            String readPositionQueueDirectory = getReadPositionQueueDirectory(queueDirectory, consumerId);
            ChronicleQueue readPositionQueue = SingleChronicleQueueBuilder.single(readPositionQueueDirectory)
                    .rollCycle(RollCycles.DAILY).build();
            chronicleQueueConcurrentHashMap.put(readPositionQueueDirectory, readPositionQueue);
            exceptAppenderConcurrentHashMap.put(readPositionQueueDirectory, readPositionQueue.acquireAppender());
            ExcerptTailer tailerReadPositionQueue = readPositionQueue.createTailer();
            long indexReadPositionEnd = tailerReadPositionQueue.toEnd().index();
            if (indexReadPositionEnd == 0) {
                tailerQueue.toStart();
            } else {
                tailerReadPositionQueue.moveToIndex(indexReadPositionEnd - 1);
                String lastReadIndexString = tailerReadPositionQueue.readText();
                if (lastReadIndexString != null) {
                    Long lastReadIndex = Long.parseLong(lastReadIndexString);
                    tailerQueue.moveToIndex(lastReadIndex + 1);
                } else {
                    tailerQueue.toStart();
                }
            }
            exceptTailerConcurrentHashMap.put(queueDirectory + consumerId, tailerQueue);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * get read position dir.
     *
     * @param queueDirectory main directory of queue
     * @param consumerId     consumer id
     * @return appender
     */
    public String getReadPositionQueueDirectory(final String queueDirectory, final String consumerId) {
        return baseDirectory + "/cqs/" + queueDirectory + "/read-position/" + consumerId;
    }

    /**
     * get main dir.
     *
     * @param queueDirectory main directory of queue
     * @return appender
     */
    public String getMainQueueDirectory(final String queueDirectory) {
        return baseDirectory + "/cqs/" + queueDirectory + "/main";
    }

    /**
     * get appender.
     *
     * @param queueDirectory main directory of queue
     * @return appender
     */
    public ExcerptAppender getAppender(final String queueDirectory) {
        return exceptAppenderConcurrentHashMap.get(getMainQueueDirectory(queueDirectory));
    }

    /**
     * get tailer.
     *
     * @param queueDirectory main directory of queue
     * @param consumerId     consumer id
     * @return tailer
     */
    public ExcerptTailer getTailer(final String queueDirectory, final String consumerId) {
        return exceptTailerConcurrentHashMap.get(queueDirectory + consumerId);
    }

    /**
     * get text.
     *
     * @param queueDirectory main directory of queue
     * @param consumerId     consumer id
     * @return text
     */
    public String readText(final String queueDirectory, final String consumerId) {
        String text = null;
        ExcerptTailer tailer = getTailer(queueDirectory, consumerId);
        long index = tailer.index();
        text = tailer.readText();
        ExcerptAppender readPositionAppender = exceptAppenderConcurrentHashMap.get(getReadPositionQueueDirectory(queueDirectory, consumerId));
        readPositionAppender.writeText(String.valueOf(index));
        return text;
    }

    /**
     * get text.
     *
     * @param queueDirectory main directory of queue
     * @param text           text
     * @return text
     */
    public void writeText(final String queueDirectory, final String text) {
        ExcerptAppender appender = exceptAppenderConcurrentHashMap.get(getMainQueueDirectory(queueDirectory));
        appender.writeText(text);
    }

    /**
     * close queue.
     *
     * @param queueDirectory main directory of queue
     */
    public void closeQueue(final String queueDirectory) {
        chronicleQueueConcurrentHashMap.get(getMainQueueDirectory(queueDirectory)).close();
        System.out.println("closed queue:" + queueDirectory);
    }

}