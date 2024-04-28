DROP TABLE IF EXISTS kafka_record;
CREATE TABLE kafka_record (
    topic_name VARCHAR(500),
    topic_partition INTEGER,
    topic_offset INTEGER,
    kafka_headers JSON,
    kafka_msg_key VARCHAR(500),
    kafka_msg_value VARCHAR(5000000),
    kafka_timestamp TIMESTAMP,
    created_by VARCHAR(500),
    created_on TIMESTAMP,
    PRIMARY KEY (topic_name, topic_partition, topic_offset)
);
