package com.example.workers;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonObject;

import java.net.URL;

/**
 * Crawler.
 */
public class Crawler extends AbstractVerticle {
    /**
     * httpclient.
     */
    private HttpClient httpClient = null;
    /**
     * httpsclient.
     */
    private HttpClient httpsClient = null;

    /**
     * HttpRequest.
     */
    private HttpClientRequest httpRequest = null;
    /**
     * HttpsRequest.
     */
    private HttpClientRequest httpsRequest = null;

    @Override
    public void start() {
        httpClient = vertx.createHttpClient(new HttpClientOptions()
                .setIdleTimeout(30000)
                .setConnectTimeout(30000)
                .setKeepAlive(false)
                .setMaxPoolSize(1));
        httpsClient = vertx.createHttpClient(new HttpClientOptions()
                .setIdleTimeout(30000)
                .setConnectTimeout(30000)
                .setKeepAlive(false)
                .setMaxPoolSize(1)
                .setUseAlpn(true)
                .setSsl(true));
        setWorker();
    }

    @Override
    public void stop() {
        if (httpClient != null) {
            httpClient.close();
        }
    }


    /**
     * set worker.
     */
    private void setWorker() {
        vertx.eventBus().<JsonObject>consumer("webClient", message -> {
            JsonObject jo = message.body();
            String urlToCrawl = jo.getString("url");
            String host = jo.getString("host");
            System.out.println("url:" + urlToCrawl);
            httpClient.redirectHandler(response -> {
                String location = response.getHeader("Location");
//                    System.out.println("Header Location:" + location);
                URL u = null;
                if (location != null && httpClient != null && httpRequest.absoluteURI() != null) {
                    try {
                        if (!location.startsWith("http")) {
                            u = new URL(new URL(httpRequest.absoluteURI()), location);
                        } else {
                            u = new URL(location);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
//                        System.out.println("computed Location:" + u.toString());
                    int port = 80;
                    String protocol = u.getProtocol();
                    if (u.getPort() < 0) {
                        if ("http".equals(protocol)) {
                            port = 80;
                        } else if ("https".equals(protocol)) {
                            port = 443;
                        }
                    }
                    if (port == 443) {
                        return Future.succeededFuture(httpsClient.get(port, u.getHost(), u.getFile()));
                    } else {
                        return Future.succeededFuture(httpClient.get(port, u.getHost(), u.getFile()));
                    }
                }
                return null;
            });
            httpsClient.redirectHandler(response -> {
                String location = response.getHeader("Location");
//                    System.out.println("Header Location:https:" + location);
                URL u = null;
                if (location != null && httpsRequest != null && httpsRequest.absoluteURI() != null) {
                    try {
                        if (!location.startsWith("http")) {
                            u = new URL(new URL(httpsRequest.absoluteURI()), location);
                        } else {
                            u = new URL(location);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
//                        System.out.println("computed Location:https:" + u.toString());
                    int port = 80;
                    String protocol = u.getProtocol();
                    if (u.getPort() < 0) {
                        if ("http".equals(protocol)) {
                            port = 80;
                        } else if ("https".equals(protocol)) {
                            port = 443;
                        }
                    }
                    if (port == 443) {
                        return Future.succeededFuture(httpsClient.get(port, u.getHost(), u.getFile()));
                    } else {
                        return Future.succeededFuture(httpClient.get(port, u.getHost(), u.getFile()));
                    }
                }
                return null;
            });
            // fire
            if (urlToCrawl.startsWith("http://")) {
                httpRequest = httpClient.get(80, host, "/", response -> {
                    int statusCode = response.statusCode();
//                    System.out.println(httpRequest.absoluteURI() + ":" + statusCode);
                    response.bodyHandler(buffer -> {
                        System.out.println(response.request().absoluteURI() + ":" + statusCode + ":" + buffer.toString().length());
                        message.reply(jo.put("html", buffer.toString()).put("finalUrl", httpRequest.absoluteURI()));
                    });
                    response.exceptionHandler(ex -> {
                        System.out.println(httpRequest.absoluteURI() + ":response:exception:" + statusCode + ":" + ex.getMessage());
                        message.fail(1000, jo.put("finalUrl", httpRequest.absoluteURI()).encode());
                    });
                });
                httpRequest.exceptionHandler(ex -> {
                    System.out.println(httpRequest.absoluteURI() + ":exception:" + ex.getMessage());
                    message.fail(1000, jo.put("finalUrl", httpRequest.absoluteURI()).encode());
                });
                httpRequest.setFollowRedirects(true);
                httpRequest.setTimeout(120000L);
                httpRequest.end();
            } else {
                httpsRequest = httpsClient.get(443, host, "/", response -> {
                    int statusCode = response.statusCode();
//                    System.out.println(httpsRequest.absoluteURI() + ":https:" + statusCode);
                    response.bodyHandler(buffer -> {
                        System.out.println(response.request().absoluteURI() + ":https:" + statusCode + ":" + buffer.toString().length());
                        message.reply(jo.put("html", buffer.toString()).put("finalUrl", httpsRequest.absoluteURI()));
                    });
                    response.exceptionHandler(ex -> {
                        System.out.println(httpsRequest.absoluteURI() + ":https:response:exception:" + statusCode + ":" + ex.getMessage());
                        message.fail(1000, jo.put("finalUrl", httpsRequest.absoluteURI()).encode());
                    });
                });
                httpsRequest.exceptionHandler(ex -> {
                    System.out.println(httpsRequest.absoluteURI() + ":https:exception:" + ex.getMessage());
                    message.fail(1000, jo.put("finalUrl", httpsRequest.absoluteURI()).encode());
                });
                httpsRequest.setFollowRedirects(true);
                httpsRequest.setTimeout(120000L);
                httpsRequest.end();
            }
        });
    }
}