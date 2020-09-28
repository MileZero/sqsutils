package com.mz.sqsutils;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import lombok.RequiredArgsConstructor;

import static com.amazonaws.services.sqs.model.QueueAttributeName.ApproximateNumberOfMessages;
import static com.amazonaws.services.sqs.model.QueueAttributeName.ApproximateNumberOfMessagesDelayed;
import static com.amazonaws.services.sqs.model.QueueAttributeName.ApproximateNumberOfMessagesNotVisible;

/**
 * @author max
 */
public abstract class SqsTask implements Callable {

    protected final AmazonSQS client;

    public SqsTask(AmazonSQS client) {
        this.client = client;
    }

    protected ReceiveMessageResult receiveMessages(String sourceQueue) {
        return client.receiveMessage(
            new ReceiveMessageRequest(sourceQueue)
                .withMaxNumberOfMessages(10)
                .withAttributeNames("ApproximateFirstReceiveTimestamp"));
    }

    protected int getTotalMessageCount(String queueUrl) {
        GetQueueAttributesResult queueAttributes = client.getQueueAttributes(queueUrl, Arrays.asList(
            ApproximateNumberOfMessages.toString(),
            ApproximateNumberOfMessagesDelayed.toString(),
            ApproximateNumberOfMessagesNotVisible.toString()));
        int approximateNumberOfMessages           = getIntAttr(queueAttributes, ApproximateNumberOfMessages);
        int approximateNumberOfMessagesDelayed    = getIntAttr(queueAttributes, ApproximateNumberOfMessagesDelayed);
        int approximateNumberOfMessagesNotVisible = getIntAttr(queueAttributes, ApproximateNumberOfMessagesNotVisible);
        return approximateNumberOfMessages + approximateNumberOfMessagesDelayed + approximateNumberOfMessagesNotVisible;
    }

    int getIntAttr(GetQueueAttributesResult queueAttributes, QueueAttributeName attrName) {
        Map<String, String> attributes = queueAttributes.getAttributes();
        return getIntAttr(attributes, attrName);
    }

    int getIntAttr(Map<String, String> attributes, QueueAttributeName attrName) {
        return Integer.parseInt(attributes.getOrDefault(attrName.toString(), "0"));
    }

    protected long getLongAttr(Message message, String attrName) {
        Map<String, String> attributes = message.getAttributes();
        return getLongAttr(attributes, attrName);
    }

    protected long getLongAttr(Map<String, String> attributes, String attrName) {
        return Long.parseLong(attributes.get(attrName));
    }
}
