package com.mz.sqsutils;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.PurgeQueueResult;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import org.slf4j.Logger;

/**
 * Deletes all messages from the queue.
 */
public class PurgeQueueTask extends SqsTask {

    private final String queueUrl;
    private final Logger logger;

    public PurgeQueueTask(AmazonSQS client, String queueUrl, Logger logger) {
        super(client);
        this.queueUrl = queueUrl;
        this.logger = logger;
    }

    @Override
    public String call() {
        try {
            PurgeQueueResult result = client.purgeQueue(new PurgeQueueRequest(queueUrl));
            String response = String.format("Purge completed successfully. %s%n", result);
            logger.info(response);
            return response;
        } catch (QueueDoesNotExistException e) {
            String response = String.format("Purge failed. Queue does not exist. %s%n", queueUrl);
            logger.error(response);
            return response;
        }
    }
}
