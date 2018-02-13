# Kafka_Streaming_with_Java_Zookeeper_Configuration_Management

1. This is a kafka streaming application with java, zookeeper and configuration management. 

2. It has a kafka broker cluster having three nodes. There are eight separate file system to store the data being streaming through
   kafka.
   
3. Kafka heap size = 64gb

4. It has been handling five different types of data in five topics named -> protocol1, protocol2, protocol3, protocol4, protocol5. 
   we can add more data types into the existing kafka cluster.

5. It has 72 partitions in each topic, replication factor as 3 and 12 hour retention period.

6. config.txt and kafka-topics.properties contain more details of kafka configurations.

7. This application acts as a consumer. It expects the data present in the topic(written by producer) conform with certain avro format. 
   The avro formats or schemas are present in this repository inside "Schemas" folders. These formats are in a json file. 
   
8. It reads the avro format data and writes the raw data as a parquet file into HDFS.

9. KafkaToLoadHdfs is the main class that creates the AdapterConsumerThread instances and monitors the running threads. Assumption is
   that we will have one consumer to handle every partition - only if one of the instances go down, a single consumer instance will
   process more than one partition - This is managed by Kafka as long as all consumers for a particular topic have the same consumer
   group id.

10. AdapterConsumerThread class contains the thread that will create the Kafka consumer and start polling the topic for messages.
    Messages read from the topic are written out to a file in parquet format.
    
11. InitTopic is the main class to handle kafka management and configuration management. Inside kafka management, it can do ->
    
    a) createTopic
    
    b) changeTopicConfig
    
    c) addPartitions
    
    d) deleteTopic
    
    e) getBrokerAddresses
    
    f) listTopics
    
    g) getTopicConfig
    
    h) fetchingInformationFromZookeeper
    
    
12. Configuration management handles any configuration changes dynamically. It takes care of following ->

    a) Initialization 
    
    b) Configuration store management : creation, modification, deletion 
    
    c) Versioning
    
    d) Kafka and Zookeeper integration
    
    e) Exception handling


13. Average data volume(for kafka processing engine) of this application is 350 mb/sec.

14. Examples of kafka topics and consumer groups ->

```
[testhost bin]$ sh kafka-topics.sh --list --zookeeper zookeeper.eea:2181
protocol1
protocol2
protocol3
protocol4
protocol5
__consumer_offsets

[testhost bin]$ sh kafka-topics.sh --describe --zookeeper zookeeper.eea:2181 --topic protocol1
Topic:protocol1       PartitionCount:64       ReplicationFactor:2     Configs:retention.ms=43200000
Topic: protocol1      Partition: 0    Leader: 1002    Replicas: 1001,1002,1003     Isr: 1002,1001,1003
Topic: protocol1      Partition: 1    Leader: 1001    Replicas: 1001,1002,1003     Isr: 1001,1003,1002
Topic: protocol1      Partition: 2    Leader: 1003    Replicas: 1001,1002,1003     Isr: 1003,1002,1001
......................................................................................................
......................................................................................................

Topic: protocol1      Partition: 70   Leader: 1003    Replicas: 1001,1002,1003     Isr: 1003,1002,1001
Topic: protocol1      Partition: 71   Leader: 1002    Replicas: 1001,1002,1003     Isr: 1002,1003,1001

[testhost bin]$ sh kafka-consumer-groups.sh -new-consumer -list -bootstrap-server localhost:9092
KafkaToLoadHdfs_Group_protocol1
KafkaToLoadHdfs_Group_protocol2
KafkaToLoadHdfs_Group_protocol3
KafkaToLoadHdfs_Group_protocol4
KafkaToLoadHdfs_Group_protocol5

[testhost bin]$ sh kafka-consumer-groups.sh -new-consumer -describe -group KafkaToLoadHdfs_Group_protocol2 -bootstrap-server localhost:9092
GROUP                           TOPIC                    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET      LAG             OWNER
KafkaToLoadHdfs_Group_protocol2 protocol2                   0          952265700       956365566       4099866         consumer-2_/testhost1
KafkaToLoadHdfs_Group_protocol2 protocol2                   35         954608551       958792400       4183849         consumer-2_/testhost2
KafkaToLoadHdfs_Group_protocol2 protocol2                   4          951554785       955509228       3954443         consumer-2_/testhost3
..................................................................................................................................................
..................................................................................................................................................
KafkaToLoadHdfs_Group_protocol2 protocol2                   15         958103406       962272290       4168884         consumer-2_/testhost4
KafkaToLoadHdfs_Group_protocol2 protocol2                   71         955788824       959797469       4008645         consumer-2_/testhost5
KafkaToLoadHdfs_Group_protocol2 protocol2                   52         954749248       958819443       4070195         consumer-2_/testhost3
KafkaToLoadHdfs_Group_protocol2 protocol2                   40         961300395       965419524       4119129         consumer-2_/testhost1

```


