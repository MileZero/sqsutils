package com.mz.sqsutils;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import java.util.List;
import java.util.function.BiFunction;
import org.slf4j.Logger;


/**
 * @author max
 */
public class ReadQueueTask extends SqsTask {

    private final String           queueUrl;
    private final Logger            logger;
    private final BiFunction<Message, String, String> messageBodyConsumer;

    public ReadQueueTask(AmazonSQS client, String queueUrl, BiFunction<Message, String, String> messageBodyConsumer, Logger logger) {
        super(client);
        this.queueUrl = queueUrl;
        this.messageBodyConsumer = messageBodyConsumer;
        this.logger = logger;
    }

    @Override
    public String call() {
        int expectedMessageCount = getTotalMessageCount(queueUrl);
        if (expectedMessageCount > 10) {
            expectedMessageCount = 10;
        }
        int  readCount               = 0;
        long operationStartTimestamp = System.currentTimeMillis();

        ReceiveMessageResult sqsMessages = receiveMessages(queueUrl);
        List<Message>        messages    = sqsMessages.getMessages();

        while (!messages.isEmpty() && readCount <= expectedMessageCount) {
            for (Message message : messages) {
                long firstTimeReceivedMillis = getLongAttr(message, "ApproximateFirstReceiveTimestamp");
                if (firstTimeReceivedMillis > operationStartTimestamp) {
                    break;
                }

                messageBodyConsumer.apply(message, message.getBody());

            }
            readCount += messages.size();
            messages = receiveMessages(queueUrl).getMessages();
        }
        String response = "ReadQueueTask completed.";
        logger.info(response);
        return response;
    }

}
