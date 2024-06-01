package org.krmdemo.kafka.util.inspect;

import com.fasterxml.jackson.annotation.JsonGetter;
import lombok.NonNull;
import org.apache.commons.collections4.MapUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.Charset;
import java.util.*;

import static org.krmdemo.kafka.util.StreamUtils.toSortedMap;
import static org.krmdemo.kafka.util.inspect.JsonResult.dumpAsJson;

/**
 * A minimal self-dependant representation of the <b>kafka-record</b> in the kafka-cluster.
 * A <i>kafka-record</i> is a unit of data, that could be produced into kafka-topic
 * and located/fetched from it later as one single portion of data.
 * <p/>
 * Once the <i>kafka-record</i> is produced into kafka-topic - a unique {@link KafkaRecordKey kafka-record-key}
 * starts corresponding to it, which (unlike other persistent storages like relational databases as ORACLE, My-SQL
 * or Postgres) IS NOT A PART of a <i>kafka-record</i>.
 * <p/>
 * This class is immutable and should be instantiated using a static method {@link #kafkaRecordBuilder()}
 * for further constructing a {@link ProducerRecord} (see the method ...)
 * or using a static method {@link #fromConsumerRecord(ConsumerRecord)} from just fetched kafka-record.
 */
public class KafkaRecordBin {
    private final byte[] binaryKey;
    private final byte[] binaryValue;
    private final Map<String, byte[]> headersMap;

    public KafkaRecordBin(byte[] binaryKey, byte[] binaryValue, Map<String, byte[]> headersMap) {
        this.binaryKey = binaryKey;
        this.binaryValue = binaryValue;
        this.headersMap = headersMap;
    }

    /**
     * @return a kafka-message-key as a byte-array
     */
    public byte[] binaryKey() {
        return this.binaryKey;
    }

    /**
     * @return a kafka-message-value as a byte-array
     */
    public byte[] binaryValue() {
        return this.binaryKey;
    }

    /**
     * @return kafka-message-headers in a way that is required by kafka-producer
     */
    public Iterable<Header> headers() {
        if (MapUtils.isEmpty(headersMap)) {
            return null;
        }
        return headersMap.entrySet().stream()
            .map(e -> new RecordHeader(e.getKey(), e.getValue()))
            .map(Header.class::cast)
            .toList();
    }

    @JsonGetter("kafka-message-key")
    public String stringKey() {
        return new String(binaryKey(), Charset.defaultCharset());
    }

    @JsonGetter("kafka-message-value")
    public String stringValue() {
        return new String(binaryValue(), Charset.defaultCharset());
    }

    @JsonGetter("kafka-headers")
    public Map<String, String> headersMap() {
        return headersMap.entrySet().stream().collect(
            toSortedMap(
                Map.Entry::getKey,
                e -> new String(e.getValue(), Charset.defaultCharset())
            )
        );
    }

    /**
     * @return a string-representation of this kafka-record as JSON
     */
    public String dump() {
        // TODO: think about caching JSON for this immutable object
        return dumpAsJson(this);
    }

    @Override
    public int hashCode() {
        return dump().hashCode();
    }

    @Override
    public boolean equals(Object thatObj) {
        if (this == thatObj) return true;
        if (thatObj == null || this.getClass() != thatObj.getClass()) return false;
        KafkaRecordBin that = (KafkaRecordBin) thatObj;
        return this.dump().equals(that.dump());
    }

    @Override
    public String toString() {
        return dump();
    }

    /**
     * @param consumerRecordBin a consumer-record as Kafka-API object
     * @return an instance of {@link KafkaRecordBin}, that corresponds to passed {@link ConsumerRecord}
     */
    public static KafkaRecordBin fromConsumerRecord(ConsumerRecord<byte[],byte[]> consumerRecordBin) {
        return kafkaRecordBuilder()
            .binaryKey(consumerRecordBin.key())
            .binaryValue(consumerRecordBin.value())
            .kafkaHeaders(consumerRecordBin.headers())
            .build();
    }

    /**
     * @return a new instance of the builder to create a new instance of {@link KafkaRecordBin}
     */
    public static Builder kafkaRecordBuilder() {
        return new Builder();
    }

    /**
     * Mutable builder to create an immutable class {@link KafkaRecordBin}.
     *
     * @see <a href="https://www.digitalocean.com/community/tutorials/builder-design-pattern-in-java">
     *         Builder Design Pattern in Java
     *     </a> for details
     */
    public static class Builder {
        private byte[] binaryKey;
        private byte[] binaryValue;
        private Map<String, byte[]> headersMap;

        private Builder() {
            // force using the factory-method "kafkaRecordBuilder()"
        }

        /**
         * @param binaryKey a kafka-message-key as a byte-array
         * @return the same builder to continue the factory-chain
         */
        public Builder binaryKey(byte[] binaryKey) {
            this.binaryKey = binaryKey;
            return this;
        }

        /**
         * @param binaryValue a kafka-message-value as a byte-array
         * @return the same builder to continue the factory-chain
         */
        public Builder binaryValue(byte[] binaryValue) {
            this.binaryValue = binaryValue;
            return this;
        }

        // auxiliary methods that propose to use strings instead of byte-arrays:

        public Builder stringKey(@NonNull String stringKey) {
            return binaryKey(stringKey.getBytes(Charset.defaultCharset()));
        }

        public Builder stringValue(@NonNull String stringValue) {
            return binaryKey(stringValue.getBytes(Charset.defaultCharset()));
        }

        public Builder putHeader(String headerKey, byte[] headerValueBin) {
            if (headersMap == null) {
                headersMap = new HashMap<>();
            }
            headersMap.put(headerKey, headerValueBin);
            return this;
        }

        public Builder putHeader(String headerKey, @NonNull String headerValueStr) {
            return putHeader(headerKey, headerValueStr.getBytes(Charset.defaultCharset()));
        }

        public Builder putHeadersAll(@NonNull Map<String, String> stringHeaders) {
            stringHeaders.forEach(this::putHeader);
            return this;
        }

        public Builder headersMap(Map<String, String> headersMap) {
            this.headersMap = null;
            if (MapUtils.isEmpty(headersMap)) {
                return this;
            } else {
                return putHeadersAll(headersMap);
            }
        }

        public Builder kafkaHeaders(Headers headers) {
            if (headers == null) {
                this.headersMap = null;
            } else {
                headers.forEach(kh -> putHeader(kh.key(), kh.value()));
            }
            return this;
        }

        /**
         * Creating the instance of {@link KafkaRecordBin} - a final step in a factory-chain
         * @return an immutable instance being constructed {@link KafkaRecordBin}
         */
        public KafkaRecordBin build() {
            return new KafkaRecordBin(binaryKey, binaryValue, headersMap);
        }
    }

    public static void main(String[] args) {
        KafkaRecordBin kafkaRecord = kafkaRecordBuilder()
            .stringKey("some-kafka-message-key")
            .stringValue("some-kafka-message-value")
            .putHeader("header-one", "value-one")
            .putHeader("header-two", "value-two")
            .build();
        System.out.println(kafkaRecord);
    }
}
