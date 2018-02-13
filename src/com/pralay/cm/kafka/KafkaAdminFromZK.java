package com.pralay.cm.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import kafka.admin.AdminOperationException;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.common.TopicExistsException;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.common.requests.MetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class KafkaAdminFromZK implements KafkaAdmin, KafkaInfo, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAdminFromZK.class);
    private final ZkUtils zkUtils;

    @Inject
    public KafkaAdminFromZK(final CuratorFramework curator) {
            zkUtils = ZkUtils.apply(curator.getZookeeperClient().getCurrentConnectionString(), 1000, 1000, false);
    }

    public List<Broker> getBrokers() {
        return scala.collection.JavaConversions.seqAsJavaList(zkUtils.getAllBrokersInCluster());
    }

    @Override
    public List<String> getBrokerAddresses() {
        ImmutableList.Builder<String> brokers = ImmutableList.builder();
            for (Broker broker: getBrokers())
                for (EndPoint endPoint: scala.collection.JavaConversions.asJavaCollection(broker.endPoints().values()))
                    brokers.add(endPoint.connectionString());
        return brokers.build();
    }

    @Override
    public void createTopic(String topic, int partitions, int replicationFactor, Properties properties) {
        try {
            AdminUtils.createTopic(zkUtils, topic, partitions, Math.max(1, replicationFactor), properties, RackAwareMode.Enforced$.MODULE$);
            LOGGER.info("kafka - topic {} created partitions:{}, replicationFactor:{}", topic, partitions, replicationFactor);
        } catch (TopicExistsException e) {
            LOGGER.warn("kafka - topic {} exists", topic, e);
        }
    }

    @Override
    public void changeTopicConfig(String topic, Properties properties) {
        try {
            AdminUtils.changeTopicConfig(zkUtils, topic, properties);
            LOGGER.info("kafka - topic {} changeTopicConfig", topic);
        } catch (AdminOperationException e) {
            LOGGER.warn("kafka - topic {} does not exist", topic, e);
        }
    }

    @Override
    public void addPartitions(String topic, int numPartitions) {
        try {
            AdminUtils.addPartitions(zkUtils, topic, numPartitions, "", true, RackAwareMode.Enforced$.MODULE$);
        } catch (AdminOperationException e) {
            LOGGER.warn("kafka - topic {} does not exist", topic, e);
        }
    }

    @Override
    public Set<String> listTopics() {
        return Sets.newHashSet(JavaConversions.asJavaList(zkUtils.getAllTopics()));
    }

    @Override
    public void deleteTopic(String topic) {
        Set<String> listTopics = listTopics();
        if (listTopics.contains(topic)) {
            AdminUtils.deleteTopic(zkUtils, topic);
            LOGGER.info("kafka - topic {} marked for deletion", topic);
        }
    }

    @Override
    public Properties getTopicConfig(String topic) {
        return AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
    }


    @Override
    public MetadataResponse.TopicMetadata getTopicMetadata(String topic) {
        return AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils);
    }

    @PreDestroy
    @Override
    public void close() {
        if (zkUtils != null) {
            zkUtils.close();
        }
    }
}