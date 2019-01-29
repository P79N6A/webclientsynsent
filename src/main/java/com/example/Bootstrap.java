package com.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

import com.example.managers.TaskManager;
import com.example.workers.Crawler;
import com.example.managers.TaskExecutor;

import io.vertx.core.Handler;
import io.vertx.core.dns.AddressResolverOptions;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.SharedData;
import org.yaml.snakeyaml.Yaml;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;

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
                .setEventLoopPoolSize(10)
                .setAddressResolverOptions(
                        new AddressResolverOptions()
                                .setCacheMinTimeToLive(0)
                                .setCacheMaxTimeToLive(0)
                                .setCacheNegativeTimeToLive(0)
                                .addServer("8.8.8.8")
                                .addServer("1.1.1.1")
                                .addServer("9.9.9.9")
                                .addServer("8.8.4.4"))
                .setMetricsOptions(new DropwizardMetricsOptions().setEnabled(true).setRegistryName("gis-registry"));
        vertx = Vertx.vertx(options);
        vertx.exceptionHandler(new Handler<Throwable>() {
            @Override
            public void handle(Throwable event) {
                logger.error(event + " throws exception: " + event.getStackTrace());
            }
        });
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
                new DeploymentOptions().setInstances(crawlerCount)
                        .setConfig(new JsonObject().put("instanceName", instanceName)),
                resCrawler -> {
                    if (resCrawler.succeeded()) {
                        logger.info("craweler deployed.");
                    } else {
                        logger.error("craweler deploy failed:" + resCrawler.cause().getMessage());
                    }
                });
        vertx.deployVerticle(TaskExecutor.class.getName(), new DeploymentOptions().setInstances(crawlerCount), resTaskExecutor -> {
            if (resTaskExecutor.succeeded()) {
                logger.info("TaskExecutor deployed.");
            } else {
                logger.error("TaskExecutor deploy failed:" + resTaskExecutor.cause().getMessage());
                resTaskExecutor.cause().printStackTrace();
            }
        });
        vertx.setTimer(3000L, h -> {
            vertx.deployVerticle(TaskManager.class.getName(), new DeploymentOptions().setInstances(1), resTaskManager -> {
                if (resTaskManager.succeeded()) {
                    logger.info("Task manager deployed.");
                } else {
                    logger.error("Task manager deploy failed:" + resTaskManager.cause().getMessage());
                    resTaskManager.cause().printStackTrace();
                }
            });
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
