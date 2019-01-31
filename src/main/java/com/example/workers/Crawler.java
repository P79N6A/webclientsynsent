package com.example.workers;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;

/**
 * Crawler.
 */
public class Crawler extends AbstractVerticle {
    /**
     * webclient.
     */
    private WebClient webClient = null;

    @Override
    public void start() {
        webClient = WebClient.create(vertx, new WebClientOptions()
                .setUserAgent("example")
                .setIdleTimeout(3000)
                .setConnectTimeout(10000)
                .setHttp2KeepAliveTimeout(5)
                .setKeepAliveTimeout(5)
                .setPoolCleanerPeriod(200)
        );
        setWorker();
    }

    @Override
    public void stop() {
        if (webClient != null) {
            webClient.close();
        }
    }


    /**
     * set worker.
     */
    private void setWorker() {
        vertx.eventBus().<JsonObject>consumer("webClient", message -> {
            JsonObject jo = message.body();
            try {
                String urlToCrawl = jo.getString("url");
//                System.out.println("url:" + urlToCrawl);
                webClient.getAbs(urlToCrawl).send(ar -> {
                    if (ar.succeeded()) {
                        try {
                            HttpResponse<Buffer> response = ar.result();
                            int statusCode = response.statusCode();
                            if (statusCode == 200) {
                                message.reply(jo.put("html", response.bodyAsString()));
                            } else {
                                message.fail(1000, jo.encode());
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else {
                        message.fail(1000, jo.encode());
                        System.out.println("ERROR:" + urlToCrawl + ":" + ar.cause().getMessage());
//                        ar.cause().printS tackTrace();
                    }
                });
            } catch (Exception e) {
                message.fail(1000, jo.encode());
                String errorMsg = e.getMessage();
                System.out.println("CRAWLER:" + errorMsg);
            }
        });
    }
}
