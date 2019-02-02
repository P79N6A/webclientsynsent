package com.example.workers;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.conn.DnsResolver;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.reactor.ConnectingIOReactor;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Crawler.
 */
public class Crawler extends AbstractVerticle {
    /**
     * webclient.
     */
    private CloseableHttpAsyncClient httpclient = null;

    /**
     * PoolingNHttpClientConnectionManager.
     */
    private PoolingNHttpClientConnectionManager cm = null;

    @Override
    public void start() {
        try {
            // https://hc.apache.org/httpcomponents-asyncclient-4.1.x/httpasyncclient/examples/org/apache/http/examples/nio/client/AsyncClientConfiguration.java
            ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
            cm = new PoolingNHttpClientConnectionManager(ioReactor);
            cm.setMaxTotal(1);
            RequestConfig requestConfig = RequestConfig.custom()
                    .setMaxRedirects(16)
                    .setRedirectsEnabled(true)
                    .setRelativeRedirectsAllowed(true)
                    .setSocketTimeout(5000)
                    .setConnectTimeout(5000).build();
            httpclient = HttpAsyncClients.custom()
                    .setConnectionManager(cm)
                    .setDefaultRequestConfig(requestConfig)
                    .build();
            httpclient.start();
            setWorker();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        try {
            if (httpclient != null) {
                httpclient.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * set worker.
     */
    private void setWorker() {
        vertx.eventBus().<JsonObject>consumer("webClient", message -> {
            JsonObject jo = message.body();
            String urlToCrawl = jo.getString("url");
//            System.out.println("URL:" + urlToCrawl);
            try {
                HttpClientContext localContext = HttpClientContext.create();
                HttpGet request = new HttpGet(urlToCrawl);

                request.setHeader("User-Agent", "gis");
                Future<org.apache.http.HttpResponse> future = httpclient.execute(request, localContext, new FutureCallback<HttpResponse>() {
                    @Override
                    public void completed(final HttpResponse response) {
                        try {
                            System.out.println(request.getRequestLine() + "->" + response.getStatusLine());
                            String result = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
                            HttpHost target = localContext.getTargetHost();
                            List<URI> redirectLocations = localContext.getRedirectLocations();
                            URI location = URIUtils.resolve(request.getURI(), target, redirectLocations);
                            cm.closeExpiredConnections();
                            cm.closeIdleConnections(5, TimeUnit.SECONDS);
                            message.reply(jo.put("html", result));
                        } catch (Exception e) {
                            e.printStackTrace();
                            cm.closeExpiredConnections();
                            cm.closeIdleConnections(5, TimeUnit.SECONDS);
                            message.fail(1000, jo.encode());
                        }
                    }

                    @Override
                    public void failed(final Exception ex) {
                        System.out.println(request.getRequestLine() + "->" + ex);
                        cm.closeExpiredConnections();
                        cm.closeIdleConnections(5, TimeUnit.SECONDS);
                        message.fail(1000, jo.encode());
                    }

                    @Override
                    public void cancelled() {
                        System.out.println(request.getRequestLine() + " cancelled");
                        cm.closeExpiredConnections();
                        cm.closeIdleConnections(5, TimeUnit.SECONDS);
                        message.fail(1000, jo.encode());
                    }
                });
            } catch (Exception e) {
                System.out.println("CRAWLER:" + urlToCrawl + ":" + e.getMessage());
                message.fail(1000, jo.encode());
            }
        });
    }
}
