package org.krmdemo.kafka.util;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;

import java.util.stream.Stream;

import static java.lang.String.format;

public class KafkaUtils {

    /**
     *
     * @param topicName the name of kafka-topic
     * @param partitionNum the number of kafka-partition with the kafka-topic
     * @return an at-sign- "@"-separated string like "<code><i>topic-name</i><b>@</b><i>partition-name</i></code>"
     */
    public static String tpKey(String topicName, int partitionNum) {
        return format("%s@%d", topicName, partitionNum);
    }

    /**
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
