package com.mz.sqsutils;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mz.hei.models.model.Event;
import com.mz.jacksonutil.ObjectMapperBuilder;
import java.io.IOException;
import java.lang.StringBuilder;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.model.GetAccountSettingsRequest;
import software.amazon.awssdk.services.lambda.model.GetAccountSettingsResponse;
import software.amazon.awssdk.services.lambda.model.ServiceException;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.AccountUsage;

/**
 * SQS management utilities.
 * <ul>
 * <li>re-driving DLQ into its main queue, or more generally, moving messages from one queue to another</li>
 * <li>purging queue. Equivalent to {@code aws sqs purge-queue --queue-url name-of-queue-to-purge}</li>
 * </ul>
 */
public class Handler implements RequestHandler<SQSEvent, String> {

    private static final String CMD_REDRIVE = "REDRIVE";
    private static final String CMD_PURGE = "PURGE";
    private static final String CMD_READ = "READ";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperBuilder().withDefaults().build();
    private  AmazonSQS client;
    private static final Logger logger = LoggerFactory.getLogger(Handler.class);
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static final LambdaAsyncClient lambdaClient = LambdaAsyncClient.create();

    public Handler() {
        this.client = AmazonSQSClient.builder().withRegion(Regions.US_WEST_2).build();
    }

    @Override
    public String handleRequest(SQSEvent sqsEvent, Context context) {
        //final String fromQueue = "EARPFulfillmentEventQueueProdDLQ";
        //final String toQueue   = "EARPFulfillmentEventQueueProd";

        //final String fromQueue = "EARPFulfillmentEventQueueStagingDLQ";
        //final String toQueue   = "EARPFulfillmentEventQueueStaging";

        //final String fromQueue   = "HEIInputProdDLQ";
        //final String toQueue     = "HEIInputProd";

        //final String fromQueue   = "HEIInputStagingDLQ";
        //final String toQueue     = "HEIInputStaging";

        //final String fromQueue = "JobseekerPopulatorStatusEventProdDLQ";
        //final String toQueue   = "JobseekerPopulatorStatusEventProd";

        //final String fromQueue = "JobseekerPopulatorStatusEventStagingDLQ";
        //final String toQueue   = "JobseekerPopulatorStatusEventStaging";

        //final String fromQueue   = "MvbEarpJobStateDLQProd";
        //final String toQueue     = "MvbEarpJobStateProd";

        //final String fromQueue   = "MvbEarpJobStateDLQProd";
        //final String toQueue     = "MvbEarpJobStateProd";

        //final String fromQueue = "MvbItineraryNotificationsDLQProd";
        //final String toQueue   = "MvbItineraryNotificationsProd";

        //final String fromQueue = "MvbWorldListenerTaskEventsDLQProd";
        //final String toQueue   = "MvbWorldListenerTaskEventsProd";

        //final String fromQueue = "MvbWorldViewEventDLQProd";
        //final String toQueue   = "MvbWorldViewEventProd";

        //final String fromQueue   = "SwitchboardTaskEventDLQProd";
        //final String toQueue     = "SwitchboardTaskEventProd";

        //final String fromQueue   = "TesseractTestDLQ";
        //final String toQueue     = "TesseractTestQueue";

        //final String fromQueue = "WebhooksOutboxProdDLQ-1aaae948-7cbc-4fc2-9cbf-8e17fe8c1457_-11kujhw2lrezn";
        //final String toQueue   = "WebhooksOutboxProd-1aaae948-7cbc-4fc2-9cbf-8e17fe8c1457_-11kujhw2lrezn";

        //final String fromQueue = "WebhooksOutboxProdDLQ-2c221bfd-1a58-4494-b2b5-20dc2407acd9_1e6oje411xuqz";
        //final String toQueue   = "WebhooksOutboxProd-2c221bfd-1a58-4494-b2b5-20dc2407acd9_1e6oje411xuqz";

        //final String fromQueue = "WebhooksOutboxProdDLQ-2c221bfd-1a58-4494-b2b5-20dc2407acd9_1jlbbfi7lwqrk";
        //final String toQueue   = "WebhooksOutboxProd-2c221bfd-1a58-4494-b2b5-20dc2407acd9_1jlbbfi7lwqrk";

        //final String fromQueue = "WebhooksOutboxProdDLQ-2c221bfd-1a58-4494-b2b5-20dc2407acd9_-1itefz45dgjiy";
        //final String toQueue   = "WebhooksOutboxProd-2c221bfd-1a58-4494-b2b5-20dc2407acd9_-1itefz45dgjiy";

        //final String fromQueue = "WebhooksOutboxProdDLQ-2c221bfd-1a58-4494-b2b5-20dc2407acd9_1kxkiatx7w8kw";
        //final String toQueue   = "WebhooksOutboxProd-2c221bfd-1a58-4494-b2b5-20dc2407acd9_1kxkiatx7w8kw";

        //final String fromQueue = "WebhooksOutboxProdDLQ-2c221bfd-1a58-4494-b2b5-20dc2407acd9_1tp1ijdxdli7y";
        //final String toQueue   = "WebhooksOutboxProd-2c221bfd-1a58-4494-b2b5-20dc2407acd9_1tp1ijdxdli7y";

        //final String fromQueue = "WebhooksOutboxProdDLQ-2c221bfd-1a58-4494-b2b5-20dc2407acd9_-h3b6c9sw7abi";
        //final String toQueue   = "WebhooksOutboxProd-2c221bfd-1a58-4494-b2b5-20dc2407acd9_-h3b6c9sw7abi";

        //final String fromQueue = "WebhooksOutboxProdDLQ-2c221bfd-1a58-4494-b2b5-20dc2407acd9_h0eo9j02s7jf";
        //final String toQueue   = "WebhooksOutboxProd-2c221bfd-1a58-4494-b2b5-20dc2407acd9_h0eo9j02s7jf";

        //final String fromQueue = "WebhooksOutboxProdDLQ-2c221bfd-1a58-4494-b2b5-20dc2407acd9_-qpb1z83pbq2o";
        //final String toQueue   = "WebhooksOutboxProd-2c221bfd-1a58-4494-b2b5-20dc2407acd9_-qpb1z83pbq2o";

        //final String fromQueue = "WebhooksOutboxProdDLQ-2c221bfd-1a58-4494-b2b5-20dc2407acd9_t3pwfvymp8pe";
        //final String toQueue   = "WebhooksOutboxProd-2c221bfd-1a58-4494-b2b5-20dc2407acd9_t3pwfvymp8pe";

        //final String fromQueue = "WebhooksOutboxProdDLQ-2c221bfd-1a58-4494-b2b5-20dc2407acd9_-xsez94wiw1by";
        //final String toQueue   = "WebhooksOutboxProd-2c221bfd-1a58-4494-b2b5-20dc2407acd9_-xsez94wiw1by";

        //final String fromQueue = "WebhooksOutboxStagingDLQ-3e59b207-cea5-4b00-8035-eed1ae26e566_1gye7uq8kmowj";
        //final String toQueue   = "WebhooksOutboxStaging-3e59b207-cea5-4b00-8035-eed1ae26e566_1gye7uq8kmowj";

        //final String fromQueue = "WebhooksOutboxStagingDLQ-3e59b207-cea5-4b00-8035-eed1ae26e566_-1i8m7j46pltro";
        //final String toQueue   = "WebhooksOutboxStaging-3e59b207-cea5-4b00-8035-eed1ae26e566_-1i8m7j46pltro";

        //final String fromQueue = "WebhooksOutboxStagingDLQ-3e59b207-cea5-4b00-8035-eed1ae26e566_1q5dwsle59i0m";
        //final String toQueue   = "WebhooksOutboxStaging-3e59b207-cea5-4b00-8035-eed1ae26e566_1q5dwsle59i0m";

        //final String fromQueue = "WebhooksOutboxStagingDLQ-3e59b207-cea5-4b00-8035-eed1ae26e566_674nauguf8gj";
        //final String toQueue   = "WebhooksOutboxStaging-3e59b207-cea5-4b00-8035-eed1ae26e566_674nauguf8gj";

        //final String fromQueue = "WebhooksOutboxStagingDLQ-3e59b207-cea5-4b00-8035-eed1ae26e566_8vxo292azuyb";
        //final String toQueue   = "WebhooksOutboxStaging-3e59b207-cea5-4b00-8035-eed1ae26e566_8vxo292azuyb";

        //final String fromQueue = "WebhooksOutboxProdDLQ-3c897e84-3957-4958-b54d-d02c01b14f15_-1qy9wbgu5z0ql";
        //final String toQueue   = "WebhooksOutboxProd-3c897e84-3957-4958-b54d-d02c01b14f15_-1qy9wbgu5z0ql";

        //final String fromQueue = "WebhooksOutboxProdDLQ-c88987f9-91e8-4561-9f07-df795ef9b8f0_13c85puh20efm";
        //final String toQueue   = "WebhooksOutboxProd-c88987f9-91e8-4561-9f07-df795ef9b8f0_13c85puh20efm";

        //final String fromQueue = "WebhooksOutboxProdDLQ-c88987f9-91e8-4561-9f07-df795ef9b8f0_1421e9nff983l";
        //final String toQueue   = "WebhooksOutboxProd-c88987f9-91e8-4561-9f07-df795ef9b8f0_1421e9nff983l";

        //final String fromQueue = "WebhooksOutboxProdDLQ-c88987f9-91e8-4561-9f07-df795ef9b8f0_-1abhjo33sx7t8";
        //final String toQueue   = "WebhooksOutboxProd-c88987f9-91e8-4561-9f07-df795ef9b8f0_-1abhjo33sx7t8";

        //final String fromQueue = "WebhooksOutboxProdDLQ-c88987f9-91e8-4561-9f07-df795ef9b8f0_gc0iu89rq31a";
        //final String toQueue   = "WebhooksOutboxProd-c88987f9-91e8-4561-9f07-df795ef9b8f0_gc0iu89rq31a";

        //final String fromQueue = "WebhooksOutboxProdDLQ-ca3ffc06-b583-409f-a249-bdd014e21e31_-1hc51nerotzv1";
        //final String toQueue   = "WebhooksOutboxProd-ca3ffc06-b583-409f-a249-bdd014e21e31_-1hc51nerotzv1";

        //final String fromQueue = "WebhooksOutboxProdDLQ-ca3ffc06-b583-409f-a249-bdd014e21e31_1s2iz9b09zpoj";
        //final String toQueue   = "WebhooksOutboxProd-ca3ffc06-b583-409f-a249-bdd014e21e31_1s2iz9b09zpoj";

        //final String fromQueue = "WebhooksOutboxProdDLQ-ca3ffc06-b583-409f-a249-bdd014e21e31_-1w5lt6ihnpqg2";
        //final String toQueue   = "WebhooksOutboxProd-ca3ffc06-b583-409f-a249-bdd014e21e31_-1w5lt6ihnpqg2";

        //final String fromQueue = "WebhooksOutboxProdDLQ-ca3ffc06-b583-409f-a249-bdd014e21e31_-1wcs729jm20ek";
        //final String toQueue   = "WebhooksOutboxProd-ca3ffc06-b583-409f-a249-bdd014e21e31_-1wcs729jm20ek";

        // final String fromQueue = "WebhooksOutboxProdDLQ-ca3ffc06-b583-409f-a249-bdd014e21e31_eww8813p7mez";
        // final String toQueue = "WebhooksOutboxProd-ca3ffc06-b583-409f-a249-bdd014e21e31_eww8813p7mez";

        //final String fromQueue = "WebhooksOutboxStagingDLQ-org-id-ExternalNotificationClientIT_-fyi99d64og38";
        //final String toQueue = "WebhooksOutboxStaging-org-id-ExternalNotificationClientIT_-fyi99d64og38";

        //final String fromQueue = "WorldListenerTaskEventProdDLQ";
        //final String toQueue   = "WorldListenerTaskEventProd";

        // final String fromQueue = "WorldListenerTaskEventStageDLQ";
        // final String toQueue   = "WorldListenerTaskEventStage";

        //final String fromQueue = "WorldSeekerFromWorldProdDLQ";
        //final String toQueue   = "WorldSeekerFromWorldProd";

        //final String fromQueue = "WorldSeekerFromWorldStageDLQ";
        //final String toQueue   = "WorldSeekerFromWorldStage";

        Handler manager = new Handler();

        final ObjectMapper objectMapper = new ObjectMapperBuilder().withDefaults().build();

        Predicate<String> heiInputQueueEmptyCheck = messageBody -> {
            try {
                HeiMessage message = objectMapper.readValue(messageBody, HeiMessage.class);
                String payload = message.getPayload();
                HashMap<String, Object> event = objectMapper.readValue(payload, HashMap.class);
                event = objectMapper.readValue(event.get("payload").toString(), HashMap.class);
                boolean badStatus = messageBody.contains("https://hooks.slack.com/services/");
                DayOfWeek currentDay = Instant.now().atZone(ZoneId.systemDefault()).getDayOfWeek();
                Integer maxDayAge;
                if (Arrays.asList(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY, DayOfWeek.MONDAY, DayOfWeek.TUESDAY).contains(currentDay)) {
                    maxDayAge = 5;
                } else {
                    maxDayAge = 3;
                }
                return Instant.parse(event.get("statusTime").toString()).isAfter(Instant.now().minus(maxDayAge, ChronoUnit.DAYS))
                        && !badStatus;
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        };

        Map<String, String> options = System.getenv();

        if (options.get("command").equalsIgnoreCase(CMD_REDRIVE)) {
            String response = new RedriveQueueTask(manager.client,
                                 (String) options.get("fromQueue"),
                                 (String) options.get("toQueue"),
                                 heiInputQueueEmptyCheck,
                                 logger
                                 )
                .call();
            return response;
        } else if (options.get("command").equalsIgnoreCase(CMD_READ)) {
            Map<String, Set<String>> webhookUrlSet = new LinkedHashMap<>();
            String response = new ReadQueueTask(manager.client, (String) options.get("queue"), new GroupByWebhookUrl(webhookUrlSet), logger)
                .call();
            logger.info(String.format("%s webhooks found: %s%n", webhookUrlSet.size(),
                              String.join("\n", webhookUrlSet.keySet())));
            webhookUrlSet.keySet().forEach(key -> {
                logger.info("All messages for webhook %s", key);
                webhookUrlSet.get(key).forEach(logger::info);
            });
            return response;
        } else if (options.get("command").equalsIgnoreCase(CMD_PURGE)) {
            return new PurgeQueueTask(manager.client, (String) options.get("queue"), logger)
                .call();
        } else {
            String response = "Please specify command to execute (\"command\" in Lambda environment variables).";
            logger.error(response);
            return response;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class HeiMessage {
        private String requestId;
        private String className;
        private String payload;

        public String getPayload() {
            return this.payload;
        }
    }

    private static class GroupByWebhookUrl implements BiFunction<Message, String, String> {
        private final Map<String, Set<String>> webhookUrlSet;

        public GroupByWebhookUrl(Map<String, Set<String>> webhookUrlSet) {
            this.webhookUrlSet = webhookUrlSet;
        }

        @Override
        public String apply(Message message, String messageBody) {
            try {
                JsonNode root    = OBJECT_MAPPER.readTree(messageBody);
                String   payload = root.get("payload").textValue();
                root = OBJECT_MAPPER.readTree(payload);
                String url = root.get("url").textValue();
                payload = root.get("payload").textValue();
                root = OBJECT_MAPPER.readTree(payload);
                if (root.has("text")) {
                    payload = root.get("text").textValue();
                }

                webhookUrlSet.computeIfAbsent(url, key -> new LinkedHashSet<>())
                             .add(payload);
                return payload;
            } catch (IOException e) {
                throw new RuntimeException(
                    String.format("Unable to process message. messageId=%s,messageBody=%s%n",
                                  message.getMessageId(), messageBody));
            }
        }
    }
}
