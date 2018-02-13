package com.pralay.cm.kafka;

import com.pralay.cm.ConfigurationException;
import com.pralay.cm.APPConfigurations;
import com.pralay.cm.managed.AppSuiteResources;
import com.pralay.cm.managed.ConfigObserver;
import com.pralay.cm.managed.ConfigSubject;
import com.pralay.cm.managed.apply.ConfigDiff;
import com.pralay.cm.managed.apply.ConfigPropertyUtils;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Properties;
import java.util.Set;

public class KafkaConfigObserver implements ConfigObserver {
    public static final String KAFKA_PREFIX = AppSuiteResources.RESOURCE_TYPE_KAFKA;
    public static final String TOPIC_PREFIX = KAFKA_PREFIX + ".topic";
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConfigObserver.class);

    public static final String PARTITIONS = "partitions";
    public static final String REPLICATION_FACTOR = "replication-factor";

    private final KafkaAdmin kafkaAdmin;
    private final AppSuiteResources resources;
    private final KafkaConfigFromService kafkaConfigFromService;


    @Inject
    public KafkaConfigObserver(final KafkaAdmin kafkaAdmin, final AppSuiteResources resources, final ConfigSubject subject, final KafkaConfigFromService kafkaConfigFromService) {
        this.kafkaAdmin = kafkaAdmin;
        this.resources = resources;
        this.kafkaConfigFromService = kafkaConfigFromService;
        subject.attach(this);
    }


    @Override
    public void update(final String appsuite, final Configuration effective, final Configuration candidate) {
        ConfigDiff diff = ConfigDiff.diff(appsuite, candidate, effective);
        if(diff.isChanged("kafka")) {
            try {
                LOGGER.debug("KafkaConfigObserver - updating configuration - {}", diff);
                manageTopics(diff, ConfigPropertyUtils.elementsAfterPrefix(diff.getAddedChildren(TOPIC_PREFIX), TOPIC_PREFIX), false);
                manageTopics(diff, ConfigPropertyUtils.elementsAfterPrefix(diff.getUpdatedChildren(TOPIC_PREFIX), TOPIC_PREFIX), true);
                removeTopics(diff.getAppSuite(), ConfigPropertyUtils.elementsAfterPrefix(diff.getRemovedChildren(TOPIC_PREFIX), TOPIC_PREFIX));
            } catch(InterruptedException e) {
                /* Should not occur. */
                LOGGER.warn("KafkaConfigObserver - Wait for removal interrupted", e);
                Thread.currentThread().interrupt();
            }
        } else {
            LOGGER.info("KafkaConfigObserver - no config was changed");
        }
    }

    /**
     * Search for updated or added config parameters, and create or update topics based on them
     * @param topics Set of added or updated keys
     * @param update True: update topics, false: create topics
     */
    private void manageTopics(ConfigDiff diff, Set<String> topics, Boolean update) throws InterruptedException{
        for (String topicName: topics) {
            if (update) {
                updateTopic(
                        topicName,
                        diff.getEffective().subset(getTopicConfigPrefix(topicName)),
                        diff.getCandidate().subset(getTopicConfigPrefix(topicName)));
            } else {
                createTopic(diff.getAppSuite(), topicName, diff.getCandidate().subset(getTopicConfigPrefix(topicName)));
            }
        }
    }

    /**
     * Update a specific topic. If the number of partitions is decreased or the replication factor is changed the topic
     * needs to be recreated. For this the function needs to wait until the async removal task is finished. If the partition
     * number is increased or secondary config changes occurred the changes will be made while the topic is alive.
     * @param topicName Name of the topic
     * @param applied The already applied old configuration
     * @param candidate The new configuration
     * @throws InterruptedException
     */
    private void updateTopic(String topicName, Configuration applied, Configuration candidate)
            throws InterruptedException {
        if (getPartitions(applied) != getPartitions(candidate)) {
            throw new ConfigurationException("cannot change partitions for topic " + topicName);
        } else {
            kafkaAdmin.changeTopicConfig(topicName, getKafkaProperties(candidate));
        }
    }

    /**
     * Search for and remove topics whose config lines have been removed
     * @param topics Set of topics to be detached
     */
    private void removeTopics(final String appsuiteName, final Set<String> topics) {
        for (String topic : topics) {
            resources.detachFromAppSuite(AppSuiteResources.RESOURCE_TYPE_KAFKA, topic, appsuiteName);
        }

    }

    private void createTopic(final String appsuiteName, final String topicName, final Configuration config) {
        resources.assignToAppSuite(AppSuiteResources.RESOURCE_TYPE_KAFKA, topicName, appsuiteName);
        try {
            kafkaAdmin.createTopic(
                    topicName,
                    getPartitions(config),
                    getReplication(config),
                    getKafkaProperties(config));
        } catch (Exception e) {
            LOGGER.error("cannot create topic" + e.getMessage());
            resources.detachFromAppSuite(AppSuiteResources.RESOURCE_TYPE_KAFKA, topicName, appsuiteName);
        }
    }

    private String getTopicConfigPrefix(final String topicName) {
        return KafkaConfigFromService.getTopicPrefix(topicName);
    }

    private Properties getKafkaProperties(Configuration config) {
        return APPConfigurations.getProperties(config.subset("config"));
    }

    private int getPartitions(Configuration config) {
        return config.getInt(PARTITIONS);
    }

    private int getReplication(Configuration applied) {
        return applied.getInt(REPLICATION_FACTOR, 0);
    }
}
