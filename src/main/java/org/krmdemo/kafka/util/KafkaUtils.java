package org.krmdemo.kafka.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.stream.Stream;

import static java.lang.String.format;

/**
 * Utility-class to work with Kafka-API
 */
public class KafkaUtils {

    /**
     * Auziliary factory-method to create an instance of {@link TopicPartition} from Kafka-API
     *
     * @param topicName the name of kafka-topic
     * @param partitionNum the sequence-number of kafka-partition within the kafka-topic
     * @return a tuple of kafka-topic and kafka-partition
     */
    public static TopicPartition tp(String topicName, int partitionNum) {
        return new TopicPartition(topicName, partitionNum);
    }

    /**
     * Parsing the string-value of <b>topic-partition-key</b>.
     * <p/>
     * The operation is inverse of {@link #}
     *
     * @param tpKey an at-sign "@"-separated string like "<code><i>topic-name</i><b>@</b><i>partition-num</i></code>"
     * @return a tuple of kafka-topic and kafka-partition
     */
    public static TopicPartition fromTopicPartitionKey(String tpKey) {
        String[] parts = StringUtils.split(tpKey, "@");
        if (parts.length != 2) {
            throw new IllegalArgumentException(format(
                "invalid number of parts in topic-partition-key '%s' --> %d != 2", tpKey, parts.length));
        }
        return new TopicPartition(parts[0], Integer.parseInt(parts[1]));
    }

    /**
     * The same as {@link #tpKey(String, int)}, but accepts the Kafka-API type
     * <p/>
     * The operation is inverse to {@link #}
     *
     * @param tp a tuple of kafka-topic and kafka-partition
     * @return an at-sign "@"-separated string like "<code><i>topic-name</i><b>@</b><i>partition-num</i></code>"
     */
    public static String tpKey(TopicPartition tp) {
        return tpKey(tp.topic(), tp.partition());
    }

    /**
     * Creating a string-value of the <b>topic-partition-key</b>. It's a unique identifier
     * of kafka-partition within the kafka-topic in the whole kafka-cluster.
     *
     * @param topicName the name of kafka-topic
     * @param partitionNum the number of kafka-partition with the kafka-topic
     * @return an at-sign- "@"-separated string like "<code><i>topic-name</i><b>@</b><i>partition-name</i></code>"
     */
    public static String tpKey(String topicName, int partitionNum) {
        return format("%s@%d", topicName, partitionNum);
    }

    /**
     * @param tpList a list of {@link TopicPartition} tuples
     * @return a list of <b>topic-partition-key</b>s
     */
    public static List<String> tpKeyList(Collection<TopicPartition> tpList) {
        return tpList.stream().map(KafkaUtils::tpKey).toList();
    }

    /**
     * TODO: must be unused !!!
     *
     * @param td kafka-topic details (including list of partition's numbers)
     * @param topicName the name of kafka-topic
     * @return a stream of {@link TopicPartition topic-partition tuple}
     */
    public static Stream<TopicPartition> streamTopicPartition(TopicDescription td, String topicName) {
        return td.partitions().stream().map(tpi -> new TopicPartition(topicName, tpi.partition()));
    }

    private KafkaUtils() {
        // prohibit the creation of utility-class instance
        throw new UnsupportedOperationException("Cannot instantiate utility-class " + getClass().getName());
    }
}
