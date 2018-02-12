package com.pralay.cm.kafka;

import org.apache.kafka.common.requests.MetadataResponse;

import java.util.Properties;

/**
 * Kafka topic administration
 */
public interface KafkaAdmin {
    MetadataResponse.TopicMetadata getTopicMetadata(String topic);

    void createTopic(String topic, int partitions, int replicationFactor, Properties properties);

    void changeTopicConfig(String topic, Properties properties);

    void addPartitions(String topic, int numPartitions);

    void deleteTopic(String topic);

}
