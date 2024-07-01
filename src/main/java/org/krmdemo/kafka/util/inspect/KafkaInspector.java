package org.krmdemo.kafka.util.inspect;


import com.fasterxml.jackson.databind.ser.std.ByteArraySerializer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.krmdemo.kafka.util.inspect.JsonResult.ClusterInfo;
import org.krmdemo.kafka.util.inspect.JsonResult.OffsetRange;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.function.UnaryOperator.identity;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.krmdemo.kafka.util.KafkaUtils.tpKey;
import static org.krmdemo.kafka.util.KafkaUtils.tpKeyList;
import static org.krmdemo.kafka.util.StreamUtils.toSortedMap;
import static org.krmdemo.kafka.util.StreamUtils.toSortedSet;
import static org.krmdemo.kafka.util.inspect.JsonResult.dumpAsJson;
import static org.krmdemo.kafka.util.inspect.KafkaFutureErrors.kfGet;

/**
 * A wrapper over {@link AdminClient kafka-admin-client} that allows to reconcile the state of kafka-storage.
 */
@Slf4j
public class KafkaInspector {

    private static final DescribeClusterOptions DESCRIBE_CLUSTER_OPTIONS =
        new DescribeClusterOptions().includeAuthorizedOperations(true);
    private static final DescribeTopicsOptions DESCRIBE_TOPICS_OPTIONS =
        new DescribeTopicsOptions().includeAuthorizedOperations(false);

    private static final Map<String, Object> CLOUD_CONFIG_PROPS = new LinkedHashMap<>() {{
        put(CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips");
        put(BOOTSTRAP_SERVERS_CONFIG, "pkc-921jm.us-east-2.aws.confluent.cloud:9092");
        put(SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        put(SASL_MECHANISM, "PLAIN");
        put(SASL_JAAS_CONFIG, """
            org.apache.kafka.common.security.plain.PlainLoginModule \
            required \
            username='GDW3ZIICAZGF7ANW' \
            password='+wulJ8RvIrOBvPu2O+cdhgcwvoeBUhiNyykoJfkEYYK9x+EjwMapTOwLWVi1wRBi';""");
    }};;

    private final Map<String, Object> adminProps = new LinkedHashMap<>(CLOUD_CONFIG_PROPS);
    private final AdminClient adminClient;
    private KafkaBrokerSnapshot lastSnapshot = null;

    /**
     * @param bootstrapServers comma-separated list of kafka-brokers addresses ({@code "host:port"})
     */
    public KafkaInspector(@NonNull String bootstrapServers) {
        this.adminProps.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.adminClient = KafkaAdminClient.create(this.adminProps);
    }

    public KafkaBrokerSnapshot lastSnapshot() {
        return this.lastSnapshot;
    }

    public boolean hasLastSnapshot() {
        return lastSnapshot() != null;
    }

    /**
     * @return a snapshot of the current kafka-cluster state
     */
    public KafkaBrokerSnapshot takeSnapshot() {
        KafkaBrokerSnapshot snapshot = new KafkaBrokerSnapshot(this.adminProps);
        snapshot.setClusterInfo(this.takeClusterInfo());
        refreshTopicsMap(snapshot);
        refreshOffsets(snapshot);
        this.lastSnapshot = snapshot;
        return snapshot;
    }

    /**
     * @return a cluster-info that is a part of whole (@link {@link KafkaBrokerSnapshot kafka-snapshot}
     *         (could be used separately as a health-check end-point for the remote kafka-cluster)
     */
    public ClusterInfo takeClusterInfo() {
        return new ClusterInfo(adminClient.describeCluster(DESCRIBE_CLUSTER_OPTIONS));
    }

    public void refreshTopicsMap(KafkaBrokerSnapshot snapshot) {
        log.info("start refreshing the topics for " + snapshot);
        KafkaFutureErrors.clear();
        snapshot.getTopicsMap().clear();
        snapshot.setRefreshTopicsErrors(null);

        // in order to retrieve the information about internal topics like "__consumer_offset" the parameter
        // `new ListTopicOptions().listInternal(true)` should be provided to method `listTopics(...)`:
        ListTopicsResult ltr = adminClient.listTopics();
        Set<String> topicNames = kfGet(ltr.names()).orElse(emptySet());

        Collection<TopicListing> topicListings = kfGet(ltr.listings()).orElse(emptyList());
        Map<String, TopicListing> topicListingsMap = topicListings.stream()
            .collect(toSortedMap(TopicListing::name, identity()));
        log.info("topicListingsMap --> " + dumpAsJson(topicListingsMap));

        Map<String, KafkaFuture<TopicDescription>> topicNameValues =
            adminClient.describeTopics(topicNames, DESCRIBE_TOPICS_OPTIONS).topicNameValues();
        for (var entry : topicNameValues.entrySet()) {
            String topicName = entry.getKey();
            Optional<TopicDescription> td = kfGet(entry.getValue());
            if (td.isEmpty()) {
                log.warn("skip describing the topic '{}'", topicName);
                continue;
            }
            snapshot.getTopicsMap().put(topicName, new KafkaBrokerTopic(td.get()));
        }

        snapshot.setRefreshTopicsErrors(KafkaFutureErrors.lastErrors());
        log.debug("finish refreshing the topics for {} --> {}",
            snapshot, snapshot.getTopicsMap().keySet());
    }

    private void refreshOffsets(KafkaBrokerSnapshot snapshot) {
        log.info("start refreshing the offsets of topic-partitions for " + snapshot);
        KafkaFutureErrors.clear();
        snapshot.setRefreshOffsetsErrors(null);

        Map<TopicPartition, ListOffsetsResultInfo> tpoResMapEarliest = listOffsets(snapshot, OffsetSpec.earliest());
        Map<TopicPartition, ListOffsetsResultInfo> tpoResMapLatest = listOffsets(snapshot, OffsetSpec.latest());
        snapshot.topicPartitionStream().forEach(tp -> {
            ListOffsetsResultInfo offsetsEarliest = tpoResMapEarliest.get(tp);
            ListOffsetsResultInfo offsetsLatest = tpoResMapLatest.get(tp);
            snapshot.partitionInfo(tp).setRange(new OffsetRange(offsetsEarliest, offsetsLatest));
        });

        snapshot.setRefreshOffsetsErrors(KafkaFutureErrors.lastErrors());
        log.debug("finish refreshing the offsets of topic-partitions for {} --> {}",
            snapshot, snapshot.partitionsMap().keySet());
    }

    private Map<TopicPartition, ListOffsetsResultInfo> listOffsets(KafkaBrokerSnapshot snapshot, OffsetSpec offsetSpec) {
        Map<TopicPartition, OffsetSpec> tpoSpecMap = snapshot.topicPartitionSpecMap(offsetSpec);
        return kfGet(adminClient.listOffsets(tpoSpecMap).all()).orElse(emptyMap());
    }

    /**
     * Producing the kafka-record into the kafka-topic with passed name using automatic detection of
     * kafka-partition within that kafka-topic (which is selected accordingly to the value of
     * {@link ProducerRecord#key() kafka-message key}).
     * <p/>
     * If a kafka-record was produced successfully - the method returns a permamnent {@link KafkaRecordKey kafka-record-key},
     * which could be used later to locate and fetch this particular kafka-record from the whole kafka-cluster.
     *
     * @param topicName the name of kafka-topic
     * @param kafkaRecord a kafka-record to produce into the kafka-topic and kafka-partition
     * @return a unique identifier of kafka-record within the whole kafka-cluster
     */
    public KafkaRecordKey produce(String topicName, KafkaRecordBin kafkaRecord) {
        return this.produce(topicName, null, kafkaRecord);
    }

    /**
     * Producing the kafka-record into the kafka-topic with passed name and optional sequence-number of
     * the kafka-partition within that kafka-topic (if partition-number is missed - the kafka-broker will select
     * the partition according to value of {@link ProducerRecord#key() kafka-message key}).
     * <p/>
     * If a kafka-record was produced successfully - the method returns a permamnent {@link KafkaRecordKey kafka-record-key},
     * which could be used later to locate and fetch this particular kafka-record from the whole kafka-cluster.
     *
     * @param topicName the name of kafka-topic
     * @param partitionNum optional sequence-number of kafka-partition within the kafka-topic
     * @param kafkaRecord a kafka-record to produce into the kafka-topic and kafka-partition
     * @return a unique identifier of kafka-record within the whole kafka-cluster
     */
    public KafkaRecordKey produce(String topicName, Integer partitionNum, KafkaRecordBin kafkaRecord) {
        Map<String, Object> producerProps = new LinkedHashMap<>(adminProps);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        String destination = partitionNum == null ?
            format("topic(%s)", topicName) :
            format("topic-partition(%s)", tpKey(topicName, partitionNum));
        log.debug("producing the record to {} --> {}", destination, kafkaRecord);

        try (var producer = new KafkaProducer<byte[],byte[]>(producerProps)) {
            ProducerRecord<byte[],byte[]> producerRecord = new ProducerRecord<>(
                topicName,
                partitionNum,
                kafkaRecord.binaryKey(),
                kafkaRecord.binaryValue(),
                kafkaRecord.headers()
            );
            RecordMetadata recordMetadata = producer.send(producerRecord).get();
            return KafkaRecordKey.builder()
                .timestamp(recordMetadata.timestamp())
                .tpKey(tpKey(recordMetadata.topic(), recordMetadata.partition()))
                .offset(recordMetadata.offset())
                .build();
        } catch (ExecutionException | InterruptedException ex) {
            throw new IllegalStateException("could not send the kafka-record to " + destination, ex);
        }
    }

    /**
     * Fetch the passed number of consumer-records from the passed set of kafka-topic-partitions
     * as the sorted map of {@link KafkaRecordKey} as a key and {@link KafkaRecordBin} as a value
     *
     * @param tpSet the set of kafka-partition tuples of type {@link TopicPartition}
     * @param countToFetch the expected number of records to fetch
     * @return the sorted map of {@link KafkaRecordKey} as a key and {@link KafkaRecordBin} as a value
     */
    public SortedMap<KafkaRecordKey, KafkaRecordBin> fetchRecords(Set<TopicPartition> tpSet, long countToFetch) {
        return fetchConsumerRecords(tpSet, countToFetch).stream()
            .collect(toSortedMap(
                KafkaRecordKey::fromConsumerRecord,
                KafkaRecordBin::fromConsumerRecord));
    }

    /**
     * Fetch the passed number of consumer-records from the passed set of kafka-topic-partitions
     * as the sorted set of {@link KafkaRecordEntry} elements.
     *
     * @param tpSet the set of kafka-partition tuples of type {@link TopicPartition}
     * @param countToFetch the expected number of records to fetch
     * @return the sorted set of {@link KafkaRecordEntry}
     */
    public SortedSet<KafkaRecordEntry> fetchRecordEntries(Set<TopicPartition> tpSet, long countToFetch) {
        return fetchConsumerRecords(tpSet, countToFetch).stream()
            .map(KafkaRecordEntry::fromConsumerRecord)
            .collect(toSortedSet());
    }

    /**
     * Fetch the passed number of consumer-records from the passed set of kafka-topic-partitions
     * as the list of Kafka-API objects of type {@link ConsumerRecord}.
     * <p/>
     * This method is intended to simplify the usage of Kafka-API - could be made {@code private} in the future.
     * <p/>
     * TODO: introduce the constants (for poll-timeout, ...)
     *
     * @param tpSet the set of kafka-partition tuples of type {@link TopicPartition}
     * @param countToFetch the expected number of records to fetch
     * @return the list of {@link ConsumerRecords (other methods of this class can be used to convert the returned list)}
     */
    public List<ConsumerRecord<byte[],byte[]>> fetchConsumerRecords(Set<TopicPartition> tpSet, long countToFetch) {
        if (CollectionUtils.isEmpty(tpSet)) {
            log.warn("fetchConsumerRecords: an empty set of kafka-topic-partitions is passed");
            return emptyList();
        }
        log.debug("fetchConsumerRecords: fetching from tpSet --> {}", tpKeyList(tpSet));

        Map<String, Object> consumerProps = new LinkedHashMap<>(adminProps);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 0);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);  // <-- the default value is 500
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-inspector-consumer-group");
        consumerProps.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "kafka-inspector-consumer-instance");

        List<ConsumerRecord<byte[],byte[]>> resultList = new ArrayList<>((int)countToFetch);
        try (var consumer = new KafkaConsumer<byte[],byte[]>(consumerProps)) {
            consumer.assign(tpSet);
            consumer.seekToBeginning(tpSet);  // <-- maybe it's redundant because of AUTO_OFFSET_RESET_CONFIG value
            while (countToFetch > 0) {
                ConsumerRecords<byte[],byte[]> records = consumer.poll(Duration.ofMillis(1000L));
                records.forEach(resultList::add);
                log.debug("fetching {} records; total fecthed so far: {}", records.count(), resultList.size());
                countToFetch -= records.count();
            }
        } catch (Exception ex) {
            throw new IllegalStateException("ac exception while consuming the records", ex);
        }
        log.debug("fetchConsumerRecords: {} records are fetched from {}", resultList.size(), tpKeyList(tpSet));
        return resultList;
    }
}
