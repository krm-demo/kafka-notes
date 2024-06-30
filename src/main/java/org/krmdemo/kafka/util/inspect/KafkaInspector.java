package org.krmdemo.kafka.util.inspect;


import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.krmdemo.kafka.util.inspect.JsonResult.ClusterInfo;
import org.krmdemo.kafka.util.inspect.JsonResult.OffsetRange;

import java.util.*;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.function.UnaryOperator.identity;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.krmdemo.kafka.util.StreamUtils.toSortedMap;
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
}
