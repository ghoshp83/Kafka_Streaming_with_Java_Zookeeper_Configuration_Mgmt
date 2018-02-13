package com.pralay.cm.kafka;

import com.pralay.cm.managed.AppSuiteResources;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.kafka.common.requests.MetadataResponse;

import javax.inject.Inject;
import java.util.Properties;
import java.util.Set;

public class KafkaConfigFromService {

    private final KafkaAdmin kafkaAdmin;
    private final KafkaInfo kafkaInfo;
    private final AppSuiteResources resources;

    @Inject
    public KafkaConfigFromService(final KafkaAdmin kafkaAdmin, KafkaInfo kafkaInfo, final AppSuiteResources resources) {
        this.kafkaAdmin = kafkaAdmin;
        this.kafkaInfo = kafkaInfo;
        this.resources = resources;
    }

    public Configuration getConfigFromService(String appsuite) {
        Set<String> currentAppSuiteKafkaTopics = Sets.newHashSet(kafkaInfo.listTopics());
        currentAppSuiteKafkaTopics.retainAll(resources.getResources(appsuite, AppSuiteResources.RESOURCE_TYPE_KAFKA));
        return getConfig(currentAppSuiteKafkaTopics, kafkaAdmin, kafkaInfo);
    }

    public static Configuration getConfig(final Set<String> topics, final KafkaAdmin admin, final KafkaInfo info) {
        ImmutableMap.Builder<String, String> configBuilder = ImmutableMap.builder();
        for (String topic: topics) {
            Properties p = info.getTopicConfig(topic);
            for (String key: p.stringPropertyNames()) {
                configBuilder.put(getTopicPrefix(topic) + ".config." + key, p.getProperty(key));
            }
            MetadataResponse.TopicMetadata topicMetadata = admin.getTopicMetadata(topic);
            configBuilder.put(getTopicPrefix(topic) + "." + KafkaConfigObserver.PARTITIONS, ""+topicMetadata.partitionMetadata().size());
            configBuilder.put(getTopicPrefix(topic) + "." + KafkaConfigObserver.REPLICATION_FACTOR,
                    Integer.toString(
                        topicMetadata.partitionMetadata().size() > 0 ? topicMetadata.partitionMetadata().get(0).replicas().size() : 1));
        }
        return new MapConfiguration(configBuilder.build());
    }

    public static String getTopicPrefix(String topic) {
        return KafkaConfigObserver.TOPIC_PREFIX + "." +topic;
    }
}
