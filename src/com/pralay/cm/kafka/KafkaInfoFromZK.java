package com.pralay.cm.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.ClientUtils;
import org.apache.zookeeper.KeeperException;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.*;

public class KafkaInfoFromZK implements KafkaInfo {
    public static final String BROKERS_IDS = "/brokers/ids";
    public static final String TOPIC_IDS = "/brokers/topics";
    public static final String TOPIC_CONFIG = "/config/topics";

    private static Gson GSON = new Gson();

    private final CuratorFramework curator;

    @Inject
    public KafkaInfoFromZK(CuratorFramework curator) {
        this.curator = curator;
    }

    @Override
    public List<String> getBrokerAddresses() {
        try {
            Set<String> brokerInfos = ImmutableSet.copyOf(curator.getChildren().forPath(BROKERS_IDS));
            ImmutableList.Builder<String> ret = ImmutableList.builder();
            for (String broker: brokerInfos) {
                BrokerInfo info = GSON.fromJson(new String(curator.getData().forPath(BROKERS_IDS + "/" + broker)), BrokerInfo.class);
                ret.add(info.getHost() + ":" + info.getPort());
            }
            return ret.build();
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptyList();
        } catch (Exception e) {
            throw new IllegalStateException("cannot retrieve list of brokers " + e.getMessage(), e);
        }
    }

    @Override
    public Set<String> listTopics() {
        try {
//            return ImmutableSet.copyOf(Cluster.bootstrap(getAddresses(getBrokerAddresses())).topics());
            return ImmutableSet.copyOf(curator.getChildren().forPath(TOPIC_IDS));
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptySet();
        } catch (Exception e) {
            throw new IllegalStateException("cannot retrieve list of topics " + e.getMessage(), e);
        }
    }

    private List<InetSocketAddress> getAddresses(List<String> brokerAddresses) {
        return ClientUtils.parseAndValidateAddresses(getBrokerAddresses());
    }

    @Override
    public Properties getTopicConfig(String topic) {
        try {
            TopicConfig config = GSON.fromJson(new String(curator.getData().forPath(TOPIC_CONFIG + "/" + topic)), TopicConfig.class);
            return config.getConfig();
        } catch (KeeperException.NoNodeException e) {
            throw new NoSuchElementException("cannot find topic " + topic);
        } catch (Exception e) {
            throw new IllegalStateException("cannot retrieve topic configuration " + e.getMessage(), e);
        }
    }

    private static class BrokerInfo {
        private final int version;
        private final String host;
        private final int port;
        private final int jmx_port;

        public BrokerInfo(int version, String host, int port, int jmx_port) {
            this.version = version;
            this.host = host;
            this.port = port;
            this.jmx_port = jmx_port;
        }

        public int getVersion() {
            return version;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public int getJmx_port() {
            return jmx_port;
        }
    }

    private static class TopicConfig {
        private final String version;

        private final Properties config;

        public TopicConfig(String version, Properties config) {
            this.version = version;
            this.config = config;
        }

        public String getVersion() {
            return version;
        }

        public Properties getConfig() {
            return config;
        }
    }
}
