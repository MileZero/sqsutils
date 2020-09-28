package com.mz.sqsutils;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.google.common.util.concurrent.RateLimiter;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import org.slf4j.Logger;

/**
 * Sends message from the source queue to the target queue and deletes them in the source queue.
 * Useful for re-driving DLQ into its regular queue.
 */
public class RedriveQueueTask extends SqsTask {
    /**
     * Slack has a limit for webhooks at 3200 requests per 5 minutes (10.6 rps)
     * If exceeded, webhook will be disabled.
     * At 5 rps, 10K messages will be re-driven in 33.3 min
     * At 8 rps - in 20.8 min
     */
    private static final double REDRIVE_RATE = 888.0;

    private final String            sourceQueue;
    private final String            targetQueue;
    private final Predicate<String> sendToTarget;
    private final Logger            logger;

    /**
     * @param client       SQS client
     * @param sourceQueue  queue to send messages from
     * @param targetQueue  queue to send messages to
     * @param sendToTarget if true, message will be forwarded to the target queue, otherwise, just deleted from the
     *                     source queue without forwarding
     */
    RedriveQueueTask(AmazonSQS client, String sourceQueue, String targetQueue, Predicate<String> sendToTarget, Logger logger) {
        super(client);
        this.sourceQueue = sourceQueue;
        this.targetQueue = targetQueue;
        this.sendToTarget = sendToTarget;
        this.logger = logger;
    }

    @Override
    public String call() {
        logger.info(String.format("Re-driving SQS messages from %s to %s%n", sourceQueue, targetQueue));

        if (Objects.equals(sourceQueue, targetQueue)) {
            String response = "Source and destination queues are the same.";
            logger.error(response);

            return response;
        }
        int expectedMsgCount = getTotalMessageCount(sourceQueue);
        if (expectedMsgCount == 0) {
            String response = "Source queue is empty.";
            logger.error(response);
            return response;
        }
        long redriveStartTimestamp = System.currentTimeMillis();
        int  maxSkips              = expectedMsgCount / 2;
        int  createCounter         = 0;
        int  deleteCounter         = 0;
        int  skipCounter           = 0;
        int  dropCounter           = 0;
        int  messageCount          = 0;

        logger.info(String.format("Re-driving %,d messages at %.1f requests per second.%n", expectedMsgCount, REDRIVE_RATE));
        RateLimiter rateLimiter = RateLimiter.create(REDRIVE_RATE);

        ReceiveMessageResult sqsMessages = receiveMessages(sourceQueue);
        List<Message>        messages    = sqsMessages.getMessages();
        while (!messages.isEmpty() && skipCounter <= maxSkips) {
            for (Message message : messages) {
                messageCount++;
                long firstTimeReceivedMillis = getLongAttr(message, "ApproximateFirstReceiveTimestamp");
                if (firstTimeReceivedMillis > redriveStartTimestamp) {
                    skipCounter++;
                    continue;
                }
                rateLimiter.acquire();
                if (sendToTarget.test(message.getBody())) {
                    client.sendMessage(targetQueue, message.getBody());
                    logger.info(String.format("Forwarding(%d): %s%n", messageCount, message.getBody()));
                    createCounter++;
                } else {
                    dropCounter++;
                    logger.info(String.format("Dropping(%d): %s%n", messageCount, message.getBody()));
                }
                client.deleteMessage(
                    new DeleteMessageRequest()
                        .withReceiptHandle(message.getReceiptHandle())
                        .withQueueUrl(sourceQueue));
                deleteCounter++;
            }
            messages = receiveMessages(sourceQueue).getMessages();
        }

        String response = String.format(
            "Re-drive complete in %s from %s to %s.%n" +
                "Created: %,d%n" +
                "Deleted: %,d%n" +
                "Dropped: %,d%n" +
                "Skipped: %,d%n",
            Duration.between(Instant.ofEpochMilli(redriveStartTimestamp), Instant.now()), sourceQueue, targetQueue,
            createCounter, deleteCounter, dropCounter, skipCounter);
        logger.info(response);

        return response;
    }
}
