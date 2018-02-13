package com.pralay.LoadHDFS;

import java.util.Properties;
import nl.chess.it.util.config.Config;
import nl.chess.it.util.config.MissingPropertyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConfig extends Config {
    private static final Logger log = LoggerFactory.getLogger(SimpleConfig.class.getName());
    public boolean ZK_SUPPORT = true;

    public boolean isZK_SUPPORT() {
        return this.ZK_SUPPORT;
    }

    public void setZK_SUPPORT(boolean zK_SUPPORT) {
        this.ZK_SUPPORT = zK_SUPPORT;
    }

    public SimpleConfig(String resourceName) {
        super(resourceName);
    }

    public SimpleConfig(Properties p) {
        super(p);
    }

    public int kafkaPollTimeout() {
        try {
            return this.getInt("input.kafka.poll.timeout");
        } catch (MissingPropertyException var2) {
            return 100;
        }
    }

    public String kafkaBootStrap() {
        try {
            return this.getString("input.kafka.bootstrap.servers");
        } catch (MissingPropertyException var2) {
            return "localhost:9092";
        }
    }

    public String kafkaSessionTimeOut() {
        try {
            return this.getString("input.kafka.session.timeout.ms");
        } catch (MissingPropertyException var2) {
            return "30000";
        }
    }

    public String adapterKafkaTopicNames() {
        try {
            return this.getString("input.adapter.kafka.topic.names");
        } catch (MissingPropertyException var2) {
            return "ss7";
        }
    }


    public String outputHDFSDirectory() {
        try {
            return this.getString("output.hdfs.directory");
        } catch (MissingPropertyException var2) {
            return "C:\\hdfsOut";
        }
    }

    public int getTopicBatchTimeInSeconds(String topicName) {
        try {
            return this.getInt("input.adapter."+topicName+".batch.time");
        } catch (MissingPropertyException var2) {
            return 60;
        }
    }


    public int getTopicBatchNumRecords(String topicName) {
        try {
            return this.getInt("input.adapter."+topicName+".batch.records");
        } catch (MissingPropertyException var2) {
            return 12000;
        }
    }

    public String getPartitionNameMapping(String topicName, int partitionID) {
        if(topicName == null) {
            return partitionID+"";
        }
        String propertyName = null;
        String retVal = partitionID+"";
        propertyName = "input.adapter."+topicName+".kafka.partition."+partitionID+".name";

        try {
            return this.getString(propertyName);
        } catch (MissingPropertyException var2) {
            return retVal;
        }
    }

    public String kafkaRequestTimeout() {
        try {
            return this.getString("input.kafka.request.timeout");
        } catch (MissingPropertyException var2) {
            return "400000";
        }
    }

    public String kafkaFetchMaxWait() {
        try {
            return this.getString("input.kafka.fetch.max.wait.ms");
        } catch (MissingPropertyException e) {
            return "300000";
        }
    }
}
