package org.krmdemo.kafka.util;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Stream;

import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Stream.of;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.krmdemo.kafka.util.KafkaUtils.streamTopicPartition;
import static org.krmdemo.kafka.util.StreamUtils.toLinkedMap;
import static org.krmdemo.kafka.util.StreamUtils.toSortedMap;

/**
 * This category of tests must be executed only one maven-profile "cloud" is on.
 */
@Tag("cloud")
public class ConfluentCloudTest {
    //  +------------+------------------------------------------------------------------+
    // | API Key    | GDW3ZIICAZGF7ANW                                                 |
    // | API Secret | +wulJ8RvIrOBvPu2O+cdhgcwvoeBUhiNyykoJfkEYYK9x+EjwMapTOwLWVi1wRBi |
    // +------------+------------------------------------------------------------------+

    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
    private static final Map<String, Object> PROPS_KAFKA_CLUSTER = new LinkedHashMap<>() {{
        put(CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips");
        put(BOOTSTRAP_SERVERS_CONFIG, "pkc-921jm.us-east-2.aws.confluent.cloud:9092");
        put(SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        put(SASL_MECHANISM, "PLAIN");
        put(SASL_JAAS_CONFIG, """
            org.apache.kafka.common.security.plain.PlainLoginModule \
            required \
            username='GDW3ZIICAZGF7ANW' \
            password='+wulJ8RvIrOBvPu2O+cdhgcwvoeBUhiNyykoJfkEYYK9x+EjwMapTOwLWVi1wRBi';""");
    }};

    //@Test <-- temporary commented out to verify maven-profile "cloud"
    void testProducerTopic01() throws Exception {
        try (final Producer<String, String> producer = kafkaProducer()) {
            ProducerRecord<String, String> prOne = new ProducerRecord<>("my-topic-01", 0, "key-one", "some-payload-one");
            ProducerRecord<String, String> prTwo = new ProducerRecord<>("my-topic-01", 0, "key-two", "some-payload-two");
            ProducerRecord<String, String> prThree = new ProducerRecord<>("my-topic-01", 0, "key-three", "some-payload-three");
            System.out.println("going to send:\n" +
                of(prOne, prTwo, prThree).map(it -> "- " + it).collect(joining("\n")));
            RecordMetadata prmOne = producer.send(prOne).get();
            RecordMetadata prmTwo = producer.send(prTwo).get();
            RecordMetadata prmThree = producer.send(prThree).get();
            System.out.println("records are sent:\n" +
                of(prmOne, prmTwo, prmThree).map(it -> "- " + it).collect(joining("\n")));
        }

    }

    @Test
    void testAdminClient() throws Exception {
        try (AdminClient adminClient = kafkaAdminClient()) {
            //DescribeClusterResult dcr = adminClient.describeCluster();
            ListTopicsResult ltr = adminClient.listTopics(new ListTopicsOptions().listInternal(true));
            Set<String> topicNames = ltr.names().get();
            Collection<TopicListing> topicListings = ltr.listings().get();
            Map<String, TopicListing> topicListingsMap = topicListings.stream()
                .collect(toSortedMap(TopicListing::name, identity()));
            DescribeTopicsOptions dtOpts = new DescribeTopicsOptions().includeAuthorizedOperations(true);
            for (var entry : adminClient.describeTopics(topicNames, dtOpts).topicNameValues().entrySet()) {
                String topicName = entry.getKey();
                TopicDescription td = entry.getValue().get();
                TopicListing tl = topicListingsMap.get(topicName);
                List<Integer> partitions = td.partitions().stream().map(TopicPartitionInfo::partition).toList();
                System.out.println("::" + entry.getKey() + ":: " + tl + "\n" + partitions + " --> " + td.authorizedOperations());
                Map<TopicPartition, OffsetSpec> tpoMapEarliest = streamTopicPartition(td, topicName)
                    .collect(toLinkedMap(identity(), tp -> OffsetSpec.earliest()));
                var tpToEarliest = adminClient.listOffsets(tpoMapEarliest).all().get();
                Map<TopicPartition, OffsetSpec> tpoMapLatest = streamTopicPartition(td, topicName)
                    .collect(toLinkedMap(identity(), tp -> OffsetSpec.latest()));
                var tpToLatest = adminClient.listOffsets(tpoMapLatest).all().get();
                streamTopicPartition(td, topicName).forEach(tp -> {
                    System.out.println(tp.topic() + "[" + tp.partition() + "]: ( " +
                        tpToEarliest.get(tp).offset() + " ; " +
                        tpToLatest.get(tp).offset() + " )");
                });
            }

            ListConsumerGroupsResult lcgr = adminClient.listConsumerGroups();
            System.out.println("adminClient.listConsumerGroups.all:\n" +
                lcgr.all().get().stream().map(it -> "- " + it).collect(joining("\n")));
        }
    }



    private static KafkaProducer<String, String> kafkaProducer() {
        Map<String, Object> propsProducer = new HashMap<>(PROPS_KAFKA_CLUSTER);
        propsProducer.put(ACKS_CONFIG, "all");
        return new KafkaProducer<>(propsProducer, STRING_SERIALIZER, STRING_SERIALIZER);
    }

    private static AdminClient kafkaAdminClient() {
        return KafkaAdminClient.create(PROPS_KAFKA_CLUSTER);
    }
}
