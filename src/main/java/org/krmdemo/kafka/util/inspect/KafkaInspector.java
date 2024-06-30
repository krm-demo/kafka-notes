package org.krmdemo.kafka.util.inspect;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.krmdemo.kafka.util.inspect.JsonResult.AnyError;
import org.krmdemo.kafka.util.inspect.JsonResult.ClusterInfo;

import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.emptySortedMap;
import static java.util.Collections.unmodifiableSortedMap;
import static java.util.function.UnaryOperator.identity;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.krmdemo.kafka.util.KafkaUtils.streamTopicPartition;
import static org.krmdemo.kafka.util.StreamUtils.toLinkedMap;
import static org.krmdemo.kafka.util.StreamUtils.toSortedMap;
import static org.krmdemo.kafka.util.inspect.JsonResult.dumpAsJson;
import static org.krmdemo.kafka.util.inspect.KafkaFutureErrors.kfGet;

@Slf4j
public class KafkaInspector {

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

    private static final DescribeClusterOptions DESCRIBE_CLUSTER_OTIONS =
        new DescribeClusterOptions().includeAuthorizedOperations(true);
    private static final DescribeTopicsOptions DESCRIBE_TOPICS_OTIONS =
        new DescribeTopicsOptions().includeAuthorizedOperations(true);


    private final Map<String, Object> adminProps = new LinkedHashMap<>(CLOUD_CONFIG_PROPS);
    private ClusterInfo clusterInfo = null;
    private SortedMap<String, KafkaBrokerTopic> topics = emptySortedMap();
    private List<AnyError> listTopicsErrors = null;

    private final transient AdminClient adminClient;

    public KafkaInspector(@NonNull String bootstrapServers) {
        this.adminProps.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.adminClient = KafkaAdminClient.create(this.adminProps);
    }

    @JsonProperty("bootstrapServersList")
    public List<String> bootstrapServersList() {
        String bootstrapServers = (String)this.adminProps.get(BOOTSTRAP_SERVERS_CONFIG);
        return asList(StringUtils.split(bootstrapServers, ",;"));
    }

    @JsonProperty("topics")
    public SortedMap<String, KafkaBrokerTopic> topics() {
        return unmodifiableSortedMap(topics);
    }

    public ReconcileInfo refresh() {
        this.clusterInfo = new ClusterInfo(adminClient.describeCluster(DESCRIBE_CLUSTER_OTIONS));
        ReconcileInfo reconcileInfo = new ReconcileInfo();
        reconcileInfo.prevState = this.topics;
        reconcileInfo.nextState = listTopics();
        this.topics = reconcileInfo.nextState;
        return reconcileInfo;
    }

    public SortedMap<String, KafkaBrokerTopic> listTopics() {
        log.info("start loading topics for " + bootstrapServersList());
        KafkaFutureErrors.clear();
        SortedMap<String, KafkaBrokerTopic> result = new TreeMap<>();
        ListTopicsResult ltr = adminClient.listTopics(new ListTopicsOptions().listInternal(true));
        Set<String> topicNames = kfGet(ltr.names()).orElse(emptySet());

        Collection<TopicListing> topicListings = kfGet(ltr.listings()).orElse(emptyList());
        Map<String, TopicListing> topicListingsMap = topicListings.stream()
            .collect(toSortedMap(TopicListing::name, identity()));
        log.info("topicListingsMap --> " + dumpAsJson(topicListingsMap));

        for (var entry : adminClient.describeTopics(topicNames, DESCRIBE_TOPICS_OTIONS).topicNameValues().entrySet()) {
            String topicName = entry.getKey();
            TopicDescription td = kfGet(entry.getValue()).orElse(null);
            if (td == null) {
                log.warn("skip describing the topic '{}'", topicName);
                continue;
            }
            KafkaBrokerTopic kbt = new KafkaBrokerTopic(td);
            //kbt.setTopicDescription(td);
            result.put(topicName, kbt);

            List<Integer> partitions = td.partitions().stream().map(TopicPartitionInfo::partition).toList();
            log.info("::" + entry.getKey() + ":: " + partitions  + " --> " + td.authorizedOperations());
            Map<TopicPartition, OffsetSpec> tpoMapEarliest = streamTopicPartition(td, topicName)
                .collect(toLinkedMap(identity(), tp -> OffsetSpec.earliest()));
            var tpToEarliest = kfGet(adminClient.listOffsets(tpoMapEarliest).all()).orElse(emptyMap());
            Map<TopicPartition, OffsetSpec> tpoMapLatest = streamTopicPartition(td, topicName)
                .collect(toLinkedMap(identity(), tp -> OffsetSpec.latest()));
            var tpToLatest = kfGet(adminClient.listOffsets(tpoMapLatest).all()).orElse(emptyMap());
            streamTopicPartition(td, topicName).forEach(tp -> {
                log.info(tp.topic() + "[" + tp.partition() + "]: ( " +
                    tpToEarliest.get(tp).offset() + " ; " +
                    tpToLatest.get(tp).offset() + " )");
            });
        }
        this.listTopicsErrors = KafkaFutureErrors.lastErrors();
        log.info("finish loading topics for " + bootstrapServersList() + ": " + result.size() + " were loaded");
        return result;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " - " + bootstrapServersList();
    }

    public static class ReconcileInfo {
        private SortedMap<String, KafkaBrokerTopic> prevState = emptySortedMap();
        private SortedMap<String, KafkaBrokerTopic> nextState = emptySortedMap();
    }
}
