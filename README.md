# Kafka_Streaming_with_Java_Zookeeper_Configuration_Management

1. This is a kafka streaming application with java, zookeeper and configuration management. 

2. It has a kafka broker cluster having three nodes. There are eight separate file system to store the data being streaming through kafka.

3. Kafka heap size = 64gb

4. It has been handling five different types of data in five topics named -> protocol1, protocol2, protocol3, protocol4, protocol5. 
   we can add more data types into the existing kafka cluster.

5. It has 64 partitions in each topic, replication factor as 3 and 12hour retention period.

6. config.txt and kafka-topics.properties contain more details of kafka configurations.

7. This application acts as a consumer. It expects the data present in the topic(written by producer) conform with certain avro format. 
   The avro formats or schemas are present in this repository inside "Schemas" folders. These formats are in a json file. 
   
8. It reads the avro format data and writes the raw data as a parquet file into HDFS.

10. KafkaToLoadHdfs is the main class that creates the AdapterConsumerThread instances and monitors the running threads. Assumption is
    that we will have one consumer to handle every partition - only if one of the instances go down, a single consumer instance will
    process more than one partition - This is managed by Kafka as long as all consumers for a particular topic have the same consumer
    group id.

11. AdapterConsumerThread class contains the thread that will create the Kafka consumer and start polling the topic for messages. Messages
    read from the topic are written out to a file in parquet format.
    
12. InitTopic is the main class to handle kafka management and configuration management. Inside kafka management, it can do ->
    
    a) createTopic
    
    b) changeTopicConfig
    
    c) addPartitions
    
    d) deleteTopic
    
    e) getBrokerAddresses
    
    f) listTopics
    
    g) getTopicConfig
    
    h) fetchingInformationFromZookeeper
    
13. Configuration management handles any configuration changes dynamically. It takes care of following ->

    a) Initialization 
    
    b) Configuration store management : creation, modification, deletion 
    
    c) Versioning
    
    d) Kafka and Zookeeper integration
    
    e) Exception handling
    
14. 
