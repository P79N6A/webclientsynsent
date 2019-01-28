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
                String urlToCrawl = null;
                urlToCrawl = jo.getString("url");
//                System.out.println("url:" + urlToCrawl);
                if (webClient != null) {
                    webClient.close();
                }
                webClient = WebClient.create(vertx, new WebClientOptions()
                        .setUserAgent("example")
                        .setIdleTimeout(30000)
                        .setConnectTimeout(30000)
                        .setKeepAliveTimeout(1000)
                        .setKeepAlive(false)
                        .setMaxPoolSize(1));
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
