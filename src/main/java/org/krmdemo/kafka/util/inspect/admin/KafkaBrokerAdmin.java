package org.krmdemo.kafka.util.inspect.admin;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySortedMap;
import static java.util.function.UnaryOperator.identity;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.krmdemo.kafka.util.KafkaUtils.streamTopicPartition;
import static org.krmdemo.kafka.util.StreamUtils.toLinkedMap;
import static org.krmdemo.kafka.util.StreamUtils.toSortedMap;

@Slf4j
public class KafkaBrokerAdmin {

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
    private SortedMap<String, KafkaBrokerTopic> topics = emptySortedMap();

    private final transient AdminClient adminClient;

    public KafkaBrokerAdmin(@NonNull String bootstrapServers) {
        this.adminProps.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.adminClient = KafkaAdminClient.create(this.adminProps);
    }

    @JsonProperty("bootstrapServersList")
    public List<String> bootstrapServersList() {
        String bootstrapServers = (String)this.adminProps.get(BOOTSTRAP_SERVERS_CONFIG);
        return asList(StringUtils.split(bootstrapServers, ",;"));
    }

    public ReconcileInfo refresh() {
        try {
            ReconcileInfo reconcileInfo = new ReconcileInfo();
            reconcileInfo.prevState = this.topics;
            reconcileInfo.nextState = loadTopics();
            // TODO: here should be a condition whether to accept the refresh or to continue waiting for some condition
            this.topics = reconcileInfo.nextState;
            return reconcileInfo;
        } catch (Exception ex) {
            throw new IllegalStateException("exception when refreshing " + this, ex);
        }
    }

    public SortedMap<String, KafkaBrokerTopic> loadTopics() throws Exception {
        log.info("start loading topics for " + bootstrapServersList());
        SortedMap<String, KafkaBrokerTopic> result = new TreeMap<>();
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
            KafkaBrokerTopic kbt = new KafkaBrokerTopic();
            kbt.setTopicListing(tl);
            kbt.setTopicDescription(td);
            result.put(topicName, kbt);
            List<Integer> partitions = td.partitions().stream().map(TopicPartitionInfo::partition).toList();
            log.info("::" + entry.getKey() + ":: " + tl + "\n" + partitions + " --> " + td.authorizedOperations());
            Map<TopicPartition, OffsetSpec> tpoMapEarliest = streamTopicPartition(td, topicName)
                .collect(toLinkedMap(identity(), tp -> OffsetSpec.earliest()));
            var tpToEarliest = adminClient.listOffsets(tpoMapEarliest).all().get();
            Map<TopicPartition, OffsetSpec> tpoMapLatest = streamTopicPartition(td, topicName)
                .collect(toLinkedMap(identity(), tp -> OffsetSpec.latest()));
            var tpToLatest = adminClient.listOffsets(tpoMapLatest).all().get();
            streamTopicPartition(td, topicName).forEach(tp -> {
                log.info(tp.topic() + "[" + tp.partition() + "]: ( " +
                    tpToEarliest.get(tp).offset() + " ; " +
                    tpToLatest.get(tp).offset() + " )");
            });
        }
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
