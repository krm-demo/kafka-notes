package org.krmdemo.kafka.util.inspect;

import lombok.Builder;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import java.util.*;

import static java.lang.String.format;

/**
 * A tuple of {@link KafkaRecordKey} and {@link KafkaRecordBin}
 * <p/>
 * It could be used as element of standard java-collections like {@link java.util.HashSet} or {@link java.util.TreeSet}
 * where comparison and equality are delegated to {@link #key()}.
 *
 * @param key a unique identifier of kafka-record within the whole kafka-cluster
 * @param record a binary representation of kafka-record
 */
@Builder
public record KafkaRecordEntry(KafkaRecordKey key, KafkaRecordBin record) implements Comparable<KafkaRecordEntry> {

    /**
     * A helper factory-method to create an instance of kafka-record-entry from {@link ConsumerRecord}
     *
     * @param cr a consumed kafka-record as Kafka-API object
     * @return an instance of kafka-record-entry
     */
    public static KafkaRecordEntry fromConsumerRecord(ConsumerRecord<byte[],byte[]> cr) {
        return builder()
            .key(KafkaRecordKey.fromConsumerRecord(cr))
            .record(KafkaRecordBin.fromConsumerRecord(cr))
            .build();
    }

    /**
     * @param topicName the name of kafka-topic
     * @param partitionNum the sequence-number of kafka-partition within that kafka-topic
     * @return a new instance of corresponding {@link ConsumerRecord}
     */
    public ConsumerRecord<byte[],byte[]> consumerRecord(String topicName, int partitionNum) {
        return new ConsumerRecord<>(
            topicName,
            partitionNum,
            this.key().offset(),
            this.key.timestamp(),
            TimestampType.CREATE_TIME,
            this.record().binaryKey().length,
            this.record().binaryValue().length,
            this.record().binaryKey(),
            this.record().binaryValue(),
            new RecordHeaders(this.record().headers()),
            Optional.empty()
        );
    }

    @Override
    public int compareTo(@NonNull KafkaRecordEntry that) {
        return this.key().compareTo(that.key());
    }

    @Override
    public int hashCode() {
        return this.key().hashCode();
    }

    @Override
    public boolean equals(Object thatObj) {
        if (this == thatObj) return true;
        if (thatObj == null || this.getClass() != thatObj.getClass()) return false;
        KafkaRecordEntry that = (KafkaRecordEntry) thatObj;
        return this.key().equals(that.key());
    }

    @Override
    public String toString() {
        return format("\"%s\": %s", this.key(), this.record());
    }
}
