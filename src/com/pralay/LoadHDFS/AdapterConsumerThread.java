package com.pralay.LoadHDFS;

/*
This class contains the thread that will create the Kafka consumer and start polling the topic for messages
Messages read from the topic are written out to a file in parquet format
 */

import java.io.*;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;


import com.pralay.core.executionfile.ExecutionFile;
import com.pralay.core.executionfile.ExecutionFileFactory;
import com.pralay.core.executionfile.FileOperation;
import com.pralay.LoadHDFS.kafka.InitTopic;
import com.pralay.SessionTestRecord;
import org.apache.hadoop.fs.FileSystem;

import com.pralay.HEADER;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

public class AdapterConsumerThread extends Thread {
    private static final Logger log = LoggerFactory.getLogger(KafkaToLoadHdfs.class.getName());
    private final int timeStep;
    private final int numRecords;
    //num recs polled / dropped per partition
    private Map<Integer,Integer> numRecsPolled = new HashMap<Integer,Integer>();
    private Map<Integer,Integer>  numRecsDropped = new HashMap<Integer,Integer>();

    private KafkaConsumer<String, String> consumer;

    private String topicName;
    //This holds the partitions and offsets read so far. It will be used to commit the offsets during a re-balance
    //so that records that have already been read are not read again
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();

    private final String hdsfsOutputDirectory;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    private volatile boolean running = true;
    private HashMap<String, PartitionRecord> rawEventRecMap = new HashMap<String, PartitionRecord>();

    private static String TMP_DIR_NAME = "tmp";

    private boolean isUnitTest=false;

    public void stopRunning() {
        this.running = false;
        log.info("In stopRunning():::Application is shutting down: Closing open files");
    }


    private class HandleRebalance implements ConsumerRebalanceListener {
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.info("Partitions revoked=" + partitions.size());
            //When partitions are being revoked, write the file and commit the offsets that this instance has read so far - so that other
            //instances which are assigned the partition do not read them again
            for (TopicPartition p : partitions) {
                log.info(p.toString());
            }
            completeWritingFiles(false);
        }

        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            //nothing to do here
        }
    }

    public AdapterConsumerThread(String topicName) {
        this.setName("AdapterConsumerThread::"+topicName);
        this.topicName = topicName;
        //Get the HDFS directory from the configuration
        hdsfsOutputDirectory = KafkaToLoadHdfs.config.outputHDFSDirectory();
        timeStep = KafkaToLoadHdfs.config.getTopicBatchTimeInSeconds(topicName);
        numRecords = KafkaToLoadHdfs.config.getTopicBatchNumRecords(topicName);
    }

    public void run() {
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaToLoadHdfs.config.kafkaBootStrap());
        //Set the group id so that multiple instances of the application will process multiple partitions of the topic
        props.put("group.id", "KafkaToLoadHdfs_Group_" + topicName);
        // handle the commit of the offset manually so that the offset is updated only if the HDFS file write
        //is successful
        props.put("enable.auto.commit", "false");
//        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", KafkaToLoadHdfs.config.kafkaSessionTimeOut());
        props.put("request.timeout.ms", KafkaToLoadHdfs.config.kafkaRequestTimeout());
        props.put("fetch.max.wait.ms", KafkaToLoadHdfs.config.kafkaFetchMaxWait());
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        if(!isUnitTest) {
            this.consumer = new KafkaConsumer(props);
        }
        int pollTimeOut = KafkaToLoadHdfs.config.kafkaPollTimeout();

        try {
            //Subscribe to the topic and also provide a ConsumerReBalanceListener
            log.info("subscribing to topic " + this.topicName);
            this.consumer.subscribe(Arrays.asList(new String[]{this.topicName}), new HandleRebalance());

        } catch (Exception e) {
            log.error("kafka Subscription error: ", e);
        } catch (Throwable e) {
            log.error("kafka Subscription error: ", e);
        }

        log.info("Subscribed to topic " + this.topicName);

        //Collect the records from the topic for the specified time OR number of records and write out the file with the
        //batch of records
        Object rawEventRec = null;
        byte[] msgValue = null;
        ConsumerRecord record = null;
        try {
            while (this.running) {
                ConsumerRecords<String, String> records = this.consumer.poll((long) pollTimeOut);
                if (records != null && !records.isEmpty()) {
                    Iterator<ConsumerRecord<String, String>> it = records.iterator();

                    while (it.hasNext()) {
                        record = (ConsumerRecord) it.next();
                        String key = record.topic() + "_" + record.partition();

                        Integer numRecsPolledPart = numRecsPolled.get(record.partition());
                        if(numRecsPolledPart == null) {
                            numRecsPolledPart = new Integer(0);

                        }
                        numRecsPolledPart++;
                        numRecsPolled.put(record.partition(),numRecsPolledPart);

                        Integer numRecsDroppedPart = numRecsDropped.get(record.partition());
                        if(numRecsDroppedPart == null) {
                            numRecsDroppedPart = new Integer(0);

                        }
                        numRecsDropped.put(record.partition(),numRecsDroppedPart);
                        currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1));

                        if (record.value() != null) {
                            log.debug("Current offset:" + record.topic() + ":" + record.partition() + ":" + record.offset());
                            try {
                                msgValue = (byte[]) record.value();
                                log.debug("First byte: Version=" + msgValue[0]);

                                log.debug("Second byte: Type=" + msgValue[1]);

                                byte[] lenArr = Arrays.copyOfRange(msgValue, 2, 6);
                                ByteBuffer wrapped = ByteBuffer.wrap(lenArr); // big-endian by default
                                int length = wrapped.getInt(); // 1
                                log.debug("Length:" + length);

                                log.debug("Format:" + msgValue[6]);

                                DatumReader<HEADER> headerDatumReader = new SpecificDatumReader<HEADER>(HEADER.class);
                                DatumReader<SessionTestRecord> sbDatumReader = new SpecificDatumReader<SessionTestRecord>(
                                        SessionTestRecord.class);

                                DatumReader<Object> recordDatumReader = new SpecificDatumReader<>();
                                //Raw event record length includes the format byte
                                ByteArrayInputStream is = new ByteArrayInputStream(Arrays.copyOfRange(msgValue, 7, 6 + length));
                                is.close();
                                BinaryDecoder in = DecoderFactory.get().binaryDecoder(is, null);

                                HEADER headerRec = new HEADER();
                                headerRec = headerDatumReader.read(headerRec, in);
                                if (headerRec == null) {
                                    log.error("Unable to decode header record: ");
                                    numRecsDroppedPart++;
                                    numRecsDropped.put(record.partition(),numRecsDroppedPart);
                                    printMsgBytes(msgValue);
                                    continue;
                                }
                                log.debug(headerRec.toString());


                                String schemaKeyStr = InitTopic.DEST_PREFIX + InitTopic.ZK_SCHEMA_NODE + "/" + headerRec.EVENT_TYPE + "_" + headerRec.EVENT_ID + "/" + headerRec.EVENT_VERSION;
                                schemaKeyStr = schemaKeyStr.toUpperCase();
                                Schema s = KafkaToLoadHdfs.schemaHashMap.get(schemaKeyStr);
                                if (s == null) {
                                    log.error("No schema found for: ", headerRec.toString(), ".Not processing this record");
                                    numRecsDroppedPart++;
                                    numRecsDropped.put(record.partition(),numRecsDroppedPart);
                                    continue;
                                }

                                recordDatumReader.setSchema(s);
                                rawEventRec = recordDatumReader.read(rawEventRec, in);

                                log.debug(rawEventRec.toString());

                                // Read the session browser record
                                is = new ByteArrayInputStream(Arrays.copyOfRange(msgValue, 13 + length, msgValue.length));
                                is.close();
                                in = DecoderFactory.get().binaryDecoder(is, in);
                                SessionTestRecord SessionTestRecord = new SessionTestRecord();
                                SessionTestRecord = sbDatumReader.read(SessionTestRecord, in);
                                log.debug(SessionTestRecord.toString());

                                //This topic will receive data from multiple partitions - but we separate the records
                                //based on the partition - so the counter for the number of records is per partition
                                PartitionRecord partitionRec = rawEventRecMap.get(key);
                                if (partitionRec == null) {
                                    partitionRec = new PartitionRecord();
                                    partitionRec.topicName = record.topic();
                                    partitionRec.partitionId = record.partition();
                                    partitionRec.startTime = System.currentTimeMillis() / 1000l;
                                    partitionRec.firstTimeStamp = SessionTestRecord.timestamp;
                                    //open the file fow write
                                    rawEventRecMap.put(key, partitionRec);
                                }
                                partitionRec.endTimeStamp = SessionTestRecord.timestamp;
                                partitionRec.numRecsDropped = numRecsDroppedPart;
                                partitionRec.numRecsPolled = numRecsPolledPart;

                                RecordData recData = partitionRec.rawEventRecMap.get(s);
                                if (recData == null) {
                                    recData = new RecordData();
                                    String partitionName = KafkaToLoadHdfs.config.getPartitionNameMapping(record.topic(), record.partition());
                                    recData.eventType = headerRec.EVENT_TYPE.toString().toLowerCase();

                                    if (System.getProperty("ECLIPSE_ENV") == null) {
                                        recData.parquetWriter = openParquetFile(s, partitionName, recData);
                                    } else { //For local testing - create the avro files since parquet files cannot be created
                                        recData.avroWriter = openAvroFile(s, partitionName, recData);
                                    }
                                    partitionRec.rawEventRecMap.put(s, recData);
                                }
                                if (System.getProperty("ECLIPSE_ENV") == null) {
                                    long current = System.currentTimeMillis();
                                    recData.parquetWriter.write(rawEventRec);
                                    log.debug("Time to write a rec in parquet =" + (System.currentTimeMillis() - current));
                                } else {
                                    recData.avroWriter.append(rawEventRec);
                                }
                                partitionRec.ctr++;
                                partitionRec.offset = record.offset();

                            } catch(IOException ie) {
                                log.error("IO Exception::",ie);
                            } catch (Exception e) {
                                log.error("Error in processing record", e);
                                numRecsDroppedPart++;
                                numRecsDropped.put(record.partition(),numRecsDroppedPart);
                                if (record != null) {
                                    log.error("Error in processing this record: Topic=" + record.topic() + ":Partition=" + record.partition() + ":Offset=" + record.offset());
                                }
                                if (msgValue != null) {
                                    printMsgBytes(msgValue);
                                }
                            }
                        }

                        //Check if file is ready to be closed
                        completeWritingFiles(true);
                    }
                } else {
                    //Nothing read from topic
                    //Check if file is ready to be closed
                    completeWritingFiles(true);
                }

                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    log.warn("Exception", e);
                }

                if(isUnitTest) {
                    //For unit test - test a single record and break out
                    break;
                }
            }
        } catch (Exception e) {
            log.error("Exception in thread:", e);

        } finally {
            log.warn("Thread: " + this.getName() + "has been nicely stopped");
            if (this.consumer != null) {
                completeWritingFiles(false);
                this.consumer.close();
            }
        }

    }

    private void printMsgBytes(byte[] msgValue) {
        if(msgValue != null) {
            String msgValueBytes = new String();
            for (int i = 0; i < msgValue.length; i++) {
                msgValueBytes += msgValue[i] + ",";
            }
            log.error("Byte array of record:"+msgValueBytes);

        }
    }

    public String getTopicName() {
        return topicName;
    }


    public OutputStream getOutputStream(String filePath) throws IOException {
        if (System.getProperty("ECLIPSE_ENV") == null) {
            ExecutionFile executionfile = ExecutionFileFactory.create(filePath, FileOperation.OVERWRITE);
            return executionfile.openForWrite();
        } else {
            File targetFile = new File(filePath);
            File parent = targetFile.getParentFile();

            if (parent != null) {
                if (!parent.exists() && !parent.mkdirs()) {
                    log.error("Couldn't create dir: " + parent);
                    throw new IllegalStateException("Couldn't create dir: " + parent);
                }
            }
            return new FileOutputStream(filePath);
        }
    }

    public Path getPathForWrite(String filePath) throws IOException {
    	ExecutionFile executionfile = ExecutionFileFactory.create(filePath, FileOperation.OVERWRITE);
        return executionfile.getPath();

    }

    public Path getPathForRead(String filePath) throws IOException {
    	ExecutionFile executionfile = ExecutionFileFactory.create(filePath, FileOperation.READ);
        return executionfile.getPath();

    }

    /**
     * This method is called when the consumer is shutting down or to check if the batch size has been reached after every record has been
     * processed
     */
    private void completeWritingFiles(boolean checkBatchComplete) {
        if(rawEventRecMap == null || rawEventRecMap.isEmpty()) {
            return;
        }
        try {
            if (log.isDebugEnabled() && !checkBatchComplete) {
                log.debug("In completeWritingFiles::" + checkBatchComplete);
                log.debug("rawEventRecMap" + rawEventRecMap);
            }
            //Use an iterator - so we can delete from the hashmap without causing concurrent modification exception
            Iterator<Map.Entry<String, PartitionRecord>> it = rawEventRecMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, PartitionRecord> item = it.next();
                if (item == null) {
                    continue;
                }

                PartitionRecord rec = item.getValue();
                if (rec == null) {
                    continue;
                }
                if (log.isDebugEnabled() && !checkBatchComplete) {
                    log.debug("PartitionRecord" + rec);
                }
                long currentTime = System.currentTimeMillis() / 1000l;
                if (!checkBatchComplete || checkBatchComplete && (currentTime - rec.startTime > (long) timeStep || rec.ctr > numRecords - 1)) {
                    //Close out the file
                    if (rec.rawEventRecMap == null) {
                        continue;
                    }
                    for (Map.Entry<Schema, RecordData> partitionItem : rec.rawEventRecMap.entrySet()) {
                        RecordData recData = partitionItem.getValue();
                        if (recData == null) {
                            continue;
                        }
                        if (log.isDebugEnabled() && !checkBatchComplete) {
                            log.debug("RecordData" + recData);
                        }
                        try {
                            if (checkBatchComplete) {
                                if (System.getProperty("ECLIPSE_ENV") == null) {
                                    if (log.isDebugEnabled() && !checkBatchComplete) {
                                        log.debug("about to close file");
                                    }
                                    if (recData.parquetWriter != null) {
                                        recData.parquetWriter.close();
                                    }

                                    if (log.isDebugEnabled() && !checkBatchComplete) {
                                        log.debug("closed the file");
                                    }
                                } else { //For local testing - create the avro files since parquet files cannot be created
                                    recData.avroWriter.close();
                                }
                            }
                        } catch (IOException e) {
                            log.error("Error in closing file", e);
                        }
                        //copy the file from temp loc to permanent loc once its been closed - this is to avoid the 0 byte issue
                        //when querying the open file through impala
                        moveFileToPermLoc(recData.openFileName, !checkBatchComplete);
                        //Once the file has been written, commit the kafka offset
                        long lastOffset = rec.offset;
                        log.info("Committing offset:" + rec.topicName + "::" + rec.partitionId + "::" + lastOffset);
                        try {
                            TopicPartition tp = new TopicPartition(rec.topicName, rec.partitionId);
                            OffsetAndMetadata offset = new OffsetAndMetadata(lastOffset + 1);
                            consumer.commitSync(Collections.singletonMap(tp,offset));
                        } catch (Exception e) {
                            //Add a catch so that if there is an issue in committing the offset due to rebalancing,code will continue as
                            log.error("Error in committing the offset",e);
                        }
                        log.info("Batch size/Timeout reached, closing file: Num of records written=" + rec.ctr + "::FileName=" + recData.openFileName + "First TimeStamp=" + rec.firstTimeStamp + "::End TimeStamp=" + rec.endTimeStamp + "::Num recs polled=" + rec.numRecsPolled + "::Num recs discarded=" + rec.numRecsDropped);
                        numRecsPolled.remove(rec.partitionId);
                        numRecsDropped.remove(rec.partitionId);

                    }
                    it.remove();
                }

            }

        } catch (Exception e) {
            log.error("Error in closing the file", e);
        }
    }

    private void moveFileToPermLoc(String openFileName,boolean shutdown) {
        try {
            String finalName = openFileName.replace(TMP_DIR_NAME, "");
            if (System.getProperty("ECLIPSE_ENV") != null) {
                File f = new File(openFileName);

                if (f.renameTo(new File(finalName))) {
                    log.info("File move is successful");
                } else {
                    log.error("File failed to move");
                }
            } else {
                if(!shutdown) {
                    ExecutionFileFactory.getHdfsFileSystem().rename(getPathForRead(openFileName), getPathForWrite(finalName));
                } else {
                    //for final shutdown use different code:
                    org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();

                    FileSystem yourFileSystem = FileSystem.get(hdfsConf);
                    yourFileSystem.rename(getPathForRead(openFileName), getPathForWrite(finalName));
                    yourFileSystem.close();
                }

            }
        } catch (Exception e) {
            log.error("Error in moving file",e);
        }
    }


    private AvroParquetWriter openParquetFile(Schema avroSchema, String partitionName, RecordData recData) throws IOException {
        AvroParquetWriter parquetWriter = null;
        try {

            String newFilename = hdsfsOutputDirectory + File.separator + recData.eventType + File.separator + TMP_DIR_NAME + File.separator +"Event" + "_" + topicName + "_" + partitionName + "_" + sdf.format(new Date()) + ".parquet";
            recData.openFileName = newFilename;
            Path outputPath = getPathForWrite(newFilename);
            if (ExecutionFileFactory.getHdfsFileSystem().exists(outputPath)) {
                //Delete the file if it exists
                log.info("Output File exists: deleting it");
                ExecutionFileFactory.getHdfsFileSystem().delete(outputPath, true);
            }
            // choose compression scheme
            CompressionCodecName compressionCodecName = CompressionCodecName.UNCOMPRESSED;
            // the ParquetWriter object that will consume Avro GenericRecords
            parquetWriter = new AvroParquetWriter(outputPath,
                    avroSchema, compressionCodecName, ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE);
            log.info("Starting to write to parquet file");

        } catch (IOException e) {
            log.error("Error in creating file", e);
            throw e;
        }
        return parquetWriter;
    }


    private DataFileWriter openAvroFile(Schema key, String partitionName, RecordData recData) throws IOException {
        SpecificDatumWriter datumWriter = new SpecificDatumWriter();
        DataFileWriter dataFileWriter = new DataFileWriter(datumWriter);
        String newFilename = hdsfsOutputDirectory + File.separator + recData.eventType + File.separator + TMP_DIR_NAME +File.separator +"Event" + "_" + topicName + "_" + partitionName + "_" + sdf.format(new Date()) + ".avro";
        OutputStream os = null;
        recData.openFileName = newFilename;

        try {
            os = getOutputStream(newFilename);
            log.info("Created data File " + newFilename);
            dataFileWriter.create(key, os);
        } catch (IOException e) {
            log.error("Error in creating the file", e);
            throw e;
        }

        return dataFileWriter;
    }

    public KafkaConsumer<String, String> getConsumer() {
        return consumer;
    }

    public void setConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    public boolean isUnitTest() {
        return isUnitTest;
    }

    public void setUnitTest(boolean unitTest) {
        isUnitTest = unitTest;
    }
}
