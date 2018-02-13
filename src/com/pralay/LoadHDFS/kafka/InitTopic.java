package com.pralay.LoadHDFS.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import com.pralay.core.ExecutionContext;
import com.pralay.core.executionfile.ExecutionFileFactory;
import com.pralay.core.configuration.ServerAddr;
import com.pralay.bigdata.zk.ZkDataService;
import com.pralay.LoadHDFS.KafkaToLoadHdfs;
import com.pralay.LoadHDFS.LogConfigurator;
import com.pralay.cm.CMModule;
import com.pralay.cm.kafka.KafkaAdmin;
import com.pralay.cm.kafka.KafkaCMManagingModule;
import com.pralay.cm.kafka.KafkaCMModule;
import com.pralay.cm.kafka.KafkaInfo;
import com.pralay.cm.managed.zookeeper.ZookeeperCMManagingModule;
import com.google.common.base.Joiner;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitTopic {
    private static String fileName = "kafka-topics.properties";
    private static Logger LOGGING;


    private static final String ZK_CONNECT_STRING = "zookeeper.testhost:2181";
    public static final String DEST_PREFIX = "/HdfsLoader"; // Start with "/"
    public static final String DEFAULT_STORAGE_NAMESPACE = "APP";
    public static final String ZK_SCHEMA_NODE = "/Schemas";

    @Inject
    KafkaInfo kafkaInfo;

    @Inject
    KafkaAdmin kafkaAdmin;

    public static void main(String[] args) throws Exception {
        try {
            //Setup the log first - so it can be used subsequently
            LogConfigurator.configure(KafkaToLoadHdfs.LOG_CONFIGURATION_FILE);
            LOGGING = LoggerFactory.getLogger(InitTopic.class.getName());

            String appPath = "./";  //For local testing from Eclipse
            String zkStr = "localhost:2181";

            if (System.getProperty("ECLIPSE_ENV") == null) {
                Injector injector = Guice.createInjector(new CMModule(), new ZookeeperCMManagingModule(),
                        new KafkaCMModule(), new KafkaCMManagingModule());
                injector.getInstance(InitTopic.class).initTopics();

                // work around , just to bypass some weird hadoop exception.
                try {
                    ExecutionFileFactory.getHdfsFileSystem();
                } catch (IOException e) {
                    LOGGING.error("failed to initialized HDFS File System :" + e.getMessage());
                    e.printStackTrace();
                }

                String appVersion = args[0];
                LOGGING.info("Starting configuration upload to zookeeper..., appVersion: " + appVersion);
                appPath = String.format("hdfs:///test_apps/kafka-to-hdfs-init-%s",
                        System.getProperty("user.name"), appVersion);

                zkStr = getZookeeperStr();
            }

            String hdfsSchemaPath = String.format(appPath + "/Schemas");

            ZkDataService zkDsForFiles = new ZkDataService(zkStr, DEFAULT_STORAGE_NAMESPACE);

            LOGGING.info("uploading to ZK store : " + "(" + DEFAULT_STORAGE_NAMESPACE + DEST_PREFIX + ZK_SCHEMA_NODE);
            zkDsForFiles.putRecursiveContentsFromFile(hdfsSchemaPath, DEST_PREFIX + ZK_SCHEMA_NODE);
            LOGGING.info("uploaded to ZK store : " + "(" + DEFAULT_STORAGE_NAMESPACE + DEST_PREFIX + ZK_SCHEMA_NODE);

            zkDsForFiles.close();
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void initTopics() {
        Integer retention = 86400000 * 7;
        InputStream input = null;
        Properties prop = new Properties();
        Set<String> topics = kafkaInfo.listTopics();
        String topicInfo = null;
        String topicsRetentionInHours = null;

        LOGGING.info("Querying Kafka");
        LOGGING.info("======================");
        LOGGING.info("Broker addresses: " + Joiner.on(", ").join(kafkaInfo.getBrokerAddresses()));
        LOGGING.info("Existing topics:  " + Joiner.on(", ").join(topics));

        try {
            input = InitTopic.class.getClassLoader().getResourceAsStream(fileName);
            // load a properties file
            prop.load(input);

            // get the property value and print it out
            topicInfo = prop.getProperty("TopicsInfo");
            topicsRetentionInHours = prop.getProperty("TopicsRetentionInHours");
            LOGGING.info("kafka-topics.properties ->  TopicsInfo value  is  " + topicInfo);
            LOGGING.info("kafka-topics.properties ->  TopicsRetentionInHours value  is  " + topicsRetentionInHours);

            if (topicsRetentionInHours != null && !topicsRetentionInHours.isEmpty())
                retention = 3600000 * Integer.parseInt(topicsRetentionInHours);

            if (topicInfo != null && !topicInfo.isEmpty()) {
                String[] splitOuterTopicsArray = topicInfo.split(";");
                for (int i = 0; i <= splitOuterTopicsArray.length - 1; i++) {
                    String[] splitInnerTopicsInfoArray = splitOuterTopicsArray[i].split(",");
                    createTopic(splitInnerTopicsInfoArray[0], Integer.parseInt(splitInnerTopicsInfoArray[1]),
                            Integer.parseInt(splitInnerTopicsInfoArray[2]), retention.toString());
                }
            }
        } catch (Exception e) {
            LOGGING.error("InitTopic -> Main method : Unable to create the topic : " + e.getMessage());
        }
    }

    public void createTopic(String topicName, int partitions, int replications, String retention) {
        Set<String> topics = kafkaInfo.listTopics();
        if (topics.contains(topicName)) {
            LOGGING.info(String.format("Topic %s already exists", topicName));
        } else {
            LOGGING.info(String.format("Creating topic %s", topicName));
            Properties config = new Properties();
            config.setProperty("retention.ms", retention);

            kafkaAdmin.createTopic(topicName, partitions, replications, config);
        }
        LOGGING.info("======================");
        LOGGING.info(String.format("Topic %s configuration:", topicName));
        printTopicConfig(topicName);
    }

    public void printTopicConfig(String topicName) {
        try {
            for (Map.Entry entry : kafkaInfo.getTopicConfig(topicName).entrySet()) {
                LOGGING.info(String.format("%s = %s", entry.getKey().toString(), entry.getValue().toString()));
            }
        } catch (NoSuchElementException e) {
            LOGGING.info("Topic does not exist");
        }
    }

    private static String getZookeeperStr() {
    	ExecutionContext context = ExecutionContext.create();
        // get zookeeper host list from App context
        List<ServerAddr> zookeeperList = context.getConfiguration().getZookeeperHosts();
        LOGGING.info("System properties : " + System.getProperties().toString());
        LOGGING.info("app config: " + context.getConfiguration().toString());
        String zookeeperStringOption =  KafkaToLoadHdfs.getConnectionStr(zookeeperList);
        if (zookeeperStringOption == null)
            zookeeperStringOption = ZK_CONNECT_STRING;
        context.close();
        return zookeeperStringOption;
    }
}