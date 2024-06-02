package org.krmdemo.kafka.util.inspect;

import lombok.Builder;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.krmdemo.kafka.util.KafkaUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;

import static java.lang.String.format;
import static java.time.Instant.ofEpochMilli;
import static java.time.LocalDateTime.ofInstant;
import static org.apache.commons.lang3.StringUtils.trimToEmpty;

/**
 * A unique identifier of a kafka-record within the whole kafka-cluster.
 * <p/>
 * This java-record represents a unique identifier for kafka-record, where:<dl>
 *     <dt>{@link #timestamp}</dt><dd>the moment where the kafka-record was produced into the kafka-topic,
 *     which is used just to sort the kafka-records by creating/fetching time; it's NOT UNIQUE neither in kafka-topic
 *     nor in kafka-partition it belongs to, but this value has the highest priority when sorting - it allows
 *     the records of the same age, but from different kafka-partitions, to go in the order they were produced</dd>
 *     <dt>{@link #tpKey}</dt><dd>string-representation of kafka-topic and kafka-partition pair,
 *     that in most cases could be created with utility-method {@link KafkaUtils#tpKey(String, int)}</dd>
 *     <dt>{@link #offset}</dt><dd>is the absolute offset of the kafka-record in the kafka-partition</dd>
 * </dl>
 * @param timestamp the moment where the kafka-record was produced into the kafka-topic
 * @param tpKey an "@"-separated string like "<code><i>topic-name</i><b>@</b><i>partition-name</i></code>"
 * @param offset an offset of the kafka-record in the kafka-partition within the kafka-topic
 */
@Builder
public record KafkaRecordKey(long timestamp, String tpKey, long offset) implements Comparable<KafkaRecordKey> {

    /**
     * A most suitable and uniform string-representation of {@link KafkaRecordKey}
     *
     * @return offset-based string like "<code><i>topic-name</i><b>@</b><i>partition-num</i><b>#</b>offset</code>"
     */
    public String offsetKey() {
        return format("%s#%d", trimToEmpty(tpKey), offset);
    }

    /**
     * @return timestamp-based string like "<code>timestamp<b>:</b><i>topic-name</i><b>@</b><i>partition-num</i><b>#</b>offset</code>"
     */
    public String tsKey() {
        if (timestamp <= 0) {
            return "-:" + offsetKey();
        } else {
            return format("%d:%s", timestamp, offsetKey());
        }
    }

    /**
     * @return {@link LocalDateTime}-based string like "<code>{local-date-time}<b>:</b><i>topic-name</i><b>@</b><i>partition-num</i><b>#</b>offset</code>"
     */
    public String ldtKey() {
        if (timestamp <= 0) {
            return "{-}:" + offsetKey();
        }
        LocalDateTime ldt = ofInstant(ofEpochMilli(timestamp), ZoneId.systemDefault());
        return format("{%s}:%s", ldt, offsetKey());
    }

    /**
     * Constructing the instance of {@link KafkaRecordKey} from a passed {@link ConsumerRecord}.
     *
     * @param cr a consumed kafka-record as Kafka-API object
     * @return a unique identifier of passed consumed kafka-record as {@link KafkaRecordKey}
     */
    public static KafkaRecordKey fromConsumerRecord(ConsumerRecord<byte[],byte[]> cr) {
        return builder()
            .timestamp(cr.timestamp())
            .tpKey(KafkaUtils.tpKey(cr.topic(), cr.partition()))
            .offset(cr.offset())
            .build();
    }

    @Override
    public int hashCode() {
        return offsetKey().hashCode();
    }

    @Override
    public boolean equals(Object thatObj) {
        if (this == thatObj) return true;
        if (thatObj == null || this.getClass() != thatObj.getClass()) return false;
        KafkaRecordKey that = (KafkaRecordKey) thatObj;
        return this.offsetKey().equals(that.offsetKey());
    }

    @Override
    public int compareTo(@NonNull KafkaRecordKey that) {
        return this.tsKey().compareTo(that.tsKey());
    }

    @Override
    public String toString() {
        return ldtKey();
    }
}
