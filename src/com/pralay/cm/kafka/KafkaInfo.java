package com.pralay.cm.kafka;

import java.util.List;
import java.util.Properties;
import java.util.Set;

public interface KafkaInfo {
    List<String> getBrokerAddresses();

    Set<String> listTopics();

    Properties getTopicConfig(String topic);
}
