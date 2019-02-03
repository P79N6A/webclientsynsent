
package com.example.managers;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;


/**
 * simple worker for test.
 */
public class TaskExecutor extends AbstractVerticle {
    /**
     * logger.
     */
    private static Logger logger = LoggerFactory.getLogger("taskDomains");

    @Override
    public void start() {
        try {
            setConsumeTaskFeed();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {

    }

    /**
     * task feed.
     */
    private void setConsumeTaskFeed() {
        vertx.eventBus().<JsonObject>consumer("taskFeed", messageTask -> {
            JsonObject taskJo = messageTask.body();
            sendToWorker("", taskJo, messageTask, 0, "webClient");
        });
    }

    /**
     * sendToWorker.
     *
     * @param instanceName       instance name
     * @param taskJo             task json object
     * @param messageTask        message task
     * @param workerEndpointSeq  work endpoint seq
     * @param workerEndpointFlow worker endpoints
     */
    private void sendToWorker(final String instanceName, final JsonObject taskJo, final Message messageTask, final int workerEndpointSeq, final String... workerEndpointFlow) {

        String workerEndpoint = workerEndpointFlow[workerEndpointSeq];
        final String endpoint = instanceName + workerEndpoint;
        vertx.eventBus().<JsonObject>send(endpoint, taskJo, new DeliveryOptions().setSendTimeout(300000L), res -> {
            if (res.succeeded()) {
                final JsonObject jo = res.result().body();
                if (workerEndpointFlow.length == workerEndpointSeq + 1) {
                    messageTask.reply(jo);
                } else {
                    sendToWorker(instanceName, jo, messageTask, workerEndpointSeq + 1, workerEndpointFlow);
                }
            } else {
                ReplyException e = (ReplyException) res.cause();
                ReplyFailure rf = e.failureType();
                switch (rf) {
                    case TIMEOUT:
                        //retry
                        sendToWorker(instanceName, taskJo, messageTask, workerEndpointSeq, workerEndpointFlow);
                        break;
                    case RECIPIENT_FAILURE:
                        messageTask.reply(new JsonObject(res.cause().getMessage()));
                        break;
                    default:
                        logger.error("sendToWorker:default:" + res.cause().getMessage());
                        break;
                }

            }
        });
    }


}