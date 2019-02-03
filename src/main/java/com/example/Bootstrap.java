package com.example;

import com.example.managers.TaskExecutor;
import com.example.managers.TaskManager;
import com.example.workers.Crawler;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.dns.AddressResolverOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.SharedData;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

/**
 * bootstrap the app.
 *
 * @author kevin wen
 */
public final class Bootstrap {
    /**
     * logger.
     */
    private static Logger logger = LoggerFactory.getLogger("synsent");
    /**
     * vertx instant.
     */
    private static Vertx vertx = null;
    /**
     * configJO .
     */
    private static JsonObject configJo = new JsonObject();
    /**
     * workerInstanceName.
     */
    private static String instanceName = null;
    /**
     * workerInstanceName.
     */
    private static int crawlerCount = 10;
    /**
     * sharedata.
     */
    private static SharedData sd = null;
    /**
     * localmaps.
     */
    private static LocalMap<String, String> configLocalMap = null;

    /**
     * avoid instant self.
     */
    private Bootstrap() {
    }

    /**
     * main method.
     *
     * @param args ll
     */
    public static void main(final String[] args) {
        System.setProperty("java.library.path", "./sigar-native-libs");
        instanceName = "macpro";
        parseConfigFile();
        setBoot();
//        vertx.setPeriodic(30000, h -> {
//            logger.info(new MetricsManager(logger, "synsent").getMetricLog());
//        });
    }

    /**
     * parse config.
     */
    private static void parseConfigFile() {
        try {
            File configFile = new File("./conf/bootstrap.conf");
            if (configFile.exists()) {
                InputStream inputStream = new FileInputStream(configFile);
                Yaml yaml = new Yaml();
                Map conifigMap = (Map) yaml.load(inputStream);
                configJo = new JsonObject(conifigMap);
                logger.info("configJo:" + configJo);
                inputStream.close();
            }
        } catch (Exception e) {
            logger.error("configFile:Excetpion:" + e.getMessage());
        }
    }

    /**
     * set config.
     */
    private static void setBoot() {
        // -Dvertx.disableDnsResolver=true
        //
        VertxOptions options = new VertxOptions()
                .setEventLoopPoolSize(1)
                .setAddressResolverOptions(
                        new AddressResolverOptions()
                                .setQueryTimeout(30000)
                                .setCacheMinTimeToLive(0)
                                .setCacheMaxTimeToLive(0)
                                .setCacheNegativeTimeToLive(0)
                                .addServer("8.8.8.8")
                                .addServer("8.8.4.4")
                                .addServer("9.9.9.9")
                                .addServer("1.1.1.1")
                                .addServer("114.114.114.114")
                )
                .setMetricsOptions(new DropwizardMetricsOptions().setEnabled(true).setRegistryName("gis-registry"));
        vertx = Vertx.vertx(options);
        sd = vertx.sharedData();
        configLocalMap = sd.getLocalMap("configLocalMap");
        crawlerCount = configJo.getInteger("crawlerCount");
        configLocalMap.put("crawlerCount", String.valueOf(crawlerCount));
        setShutdownHook();
        deployVerticles();
    }

    /**
     * set shutdown hook.
     */
    private static void setShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Running Shutdown Hook");
                undeployAllVerticles();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * deploy verticles.
     */
    private static void deployVerticles() {

        // depoly workers.
        vertx.deployVerticle(Crawler.class.getName(),
                new DeploymentOptions().setInstances(crawlerCount),
                resCrawler -> {
                    if (resCrawler.succeeded()) {
                        logger.info("craweler deployed.");
                        vertx.deployVerticle(TaskExecutor.class.getName(), new DeploymentOptions().setInstances(crawlerCount), resTaskExecutor -> {
                            if (resTaskExecutor.succeeded()) {
                                logger.info("TaskExecutor deployed.");
                                vertx.deployVerticle(TaskManager.class.getName(), new DeploymentOptions().setInstances(1), resTaskManager -> {
                                    if (resTaskManager.succeeded()) {
                                        logger.info("Task manager deployed.");
                                    } else {
                                        logger.error("Task manager deploy failed:" + resTaskManager.cause().getMessage());
                                        resTaskManager.cause().printStackTrace();
                                    }
                                });
                            } else {
                                logger.error("TaskExecutor deploy failed:" + resTaskExecutor.cause().getMessage());
                                resTaskExecutor.cause().printStackTrace();
                            }
                        });
                    } else {
                        logger.error("craweler deploy failed:" + resCrawler.cause().getMessage());
                    }
                });
    }

    /**
     * undeploy verticles.
     */
    private static void undeployAllVerticles() {
        for (String deploymentID : vertx.deploymentIDs()) {
            vertx.undeploy(deploymentID, res -> {
                if (res.succeeded()) {
                    logger.info("Undeployed Successfully:" + deploymentID);
                    System.out.println("Undeployed Successfully:" + deploymentID);
                } else {
                    logger.error("Undeployed Failed:" + deploymentID + ":" + res.cause().getMessage());
                    System.out.println("Undeployed Failed:" + deploymentID + ":" + res.cause().getMessage());
                    res.cause().printStackTrace();
                }
            });
        }
    }


}
