package com.pralay.LoadHDFS;

import com.pralay.core.executionfile.ExecutionFile;
import com.pralay.core.executionfile.ExecutionFileFactory;
import com.pralay.core.executionfile.FileOperation;
import com.pralay.HEADER;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.*;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * This class is a thread that will process the source queue which has been populated with the message objects by the AdapterConsumerThread.
 * Messages will be batched for a configurable number of seconds and then written out to HDFS
 * It will run continuously till the shutdown hook is invoked and this thread is stopped.
 *
 * Not used anymore - the write to HDFS is done in the AdapterConsumerThread
 */
@Deprecated
public class HDFSWriterThread extends Thread {
    private static final Logger log = LoggerFactory.getLogger(HDFSWriterThread.class.getName());

    private final PriorityBlockingQueue<QueueItem> source_queue;
    private final String hdsfsOutputDirectory;
    private final int partitionId;
    private volatile boolean running = true;
    private volatile boolean go = true;
    private String partitionName;
    private String topicName;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");


    public HDFSWriterThread(PriorityBlockingQueue<QueueItem> sourceQ,String topicName, int partitionId) {

        this.source_queue = sourceQ;
        partitionName = KafkaToLoadHdfs.config.getPartitionNameMapping(topicName,partitionId);
        this.topicName = topicName;
        this.partitionId = partitionId;
        //Get the HDFS directory from the configuration
        hdsfsOutputDirectory = KafkaToLoadHdfs.config.outputHDFSDirectory();
    }

    public void run() {
        long currentTime = -1L;
        Iterator it = null;
        //Time to collect records from topic before writing to a file
        int timeStep = 0;
        int numRecords = 0;
        timeStep = KafkaToLoadHdfs.config.getTopicBatchTimeInSeconds(topicName);
        numRecords = KafkaToLoadHdfs.config.getTopicBatchNumRecords(topicName);

        long offset = 0;
        Object rawEventRec = null;
        while (running) {
            if (source_queue != null && source_queue.size() > 0) {
                try {

                    go = true;
                    long startCounterTime = System.currentTimeMillis() / 1000L;

                    int ctr = 0;
                    //Collect the records from the topic for the specified time OR number of records and write out the file with the
                    //batch of records
                    HashMap<Schema,List<Object>> rawEventRecMap = new HashMap<Schema,List<Object>>();

                    while(go) {
                        QueueItem qItem = source_queue.poll(10L, TimeUnit.MILLISECONDS);
                        if (qItem != null) {
                            offset = qItem.offset;
                            try {
                                byte[] msgValue = (byte[]) qItem.item;
                                log.debug("First byte: Version="+ msgValue[0]);

                                log.debug("Second byte: Type="+ msgValue[1]);

                                byte[] lenArr = Arrays.copyOfRange(msgValue, 2, 6);
                                ByteBuffer wrapped = ByteBuffer.wrap(lenArr); // big-endian by default
                                int length = wrapped.getInt(); // 1
                                log.debug("Length:"+ length);

                                log.debug("Format:"+ msgValue[6]);

                                DatumReader<HEADER> headerDatumReader = new SpecificDatumReader<HEADER>(HEADER.class);

                                DatumReader<Object> recordDatumReader = new SpecificDatumReader<>();

                                ByteArrayInputStream is = new ByteArrayInputStream(Arrays.copyOfRange(msgValue, 7, 7 + length));
                                is.close();
                                BinaryDecoder in = DecoderFactory.get().binaryDecoder(is, null);

                                HEADER headerRec = new HEADER();
                                headerRec = headerDatumReader.read(headerRec, in);
                                log.debug(headerRec.toString());


                                String schemaKeyStr = "/HdfsLoader/" + headerRec.EVENT_TYPE + "_" + headerRec.EVENT_ID + "/" + headerRec.EVENT_VERSION;
                                Schema s = KafkaToLoadHdfs.schemaHashMap.get(schemaKeyStr);
                                if (s == null) {
                                    log.error("No schema found for: ", headerRec, ".Not processing this record");
                                    continue;
                                }

                                recordDatumReader.setSchema(s);
                                rawEventRec = recordDatumReader.read(rawEventRec, in);
                                ++ctr;
                                log.debug(rawEventRec.toString());

                                List <Object> rawEventRecList = rawEventRecMap.get(s);
                                if(rawEventRecList == null) {
                                    rawEventRecList = new ArrayList<Object>();
                                    rawEventRecMap.put(s,rawEventRecList);
                                }
                                rawEventRecList.add(rawEventRec);
                            } catch(Exception e) {
                                log.error("Error in processing this record",e);

                            }
                        } else {
                            //no queue item
                            Thread.sleep(1000);
                        }

                        currentTime = System.currentTimeMillis() / 1000L;
                        if(currentTime - startCounterTime > (long)timeStep || ctr > numRecords-1) {
                            go = false;
                        }

                    } //end while (go)
                    //create the file with the data - ideally there will be only one type of schema records coming on this
                    //topic - but just in case different schemas come in the same topic, write them to separate files

                    for(Map.Entry<Schema,List<Object>> entry : rawEventRecMap.entrySet()) {
                        if(System.getenv("ECLIPSE_ENV") == null) {
                            createParquetFile(entry.getKey(), entry.getValue());
                        } else { //For local testing - create the avro files since parquet files cannot be created
                            createAvroFile(entry.getKey(), entry.getValue());
                        }
                    }
                    //Now commit the offset since the file has been successfully written to HDFS
                    writeOffsetFile(offset);

                } catch (InterruptedException e) {
                    log.error("InterruptedException : " + e.getMessage());
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            } else {  //nothing in source_queue – wait for messages
                // sleep for ten seconds.
                try {
                    int n = 1000;
                    log.debug("source queue: " + source_queue.size()
                            + " sleeping " + n / 1000
                            + " secs waiting on new messages to arrive");
                    Thread.sleep(n);
                } catch (InterruptedException e) {
                    log.warn("InterruptedException", e);
                }
            }


        } //while running loop
    } //end of run() method

    private void createParquetFile(Schema avroSchema, List<Object> value) {
        try {
            String newFilename = hdsfsOutputDirectory + File.separator + topicName + File.separator + "Event" + "_" + topicName + "_" + partitionName + "_" + sdf.format(new Date()) + ".parquet";

            Path outputPath = getPathForWrite(newFilename);
            if (ExecutionFileFactory.getHdfsFileSystem().exists(outputPath)) {
                //Delete the file if it exists
                log.info("Output File exists: deleting it");
                ExecutionFileFactory.getHdfsFileSystem().delete(outputPath, true);
            }
            // choose compression scheme
            CompressionCodecName compressionCodecName = CompressionCodecName.UNCOMPRESSED;
            // the ParquetWriter object that will consume Avro GenericRecords
            AvroParquetWriter parquetWriter = new AvroParquetWriter(outputPath,
                    avroSchema, compressionCodecName, ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE);
            log.info("Starting to write to parquet file");

            for (Object rec : value) {
                parquetWriter.write(rec);
            }
            parquetWriter.close();
        } catch (Exception e) {
            log.error("Exception in createParquetFile",e);
        } catch (Throwable e) {
            log.error("Exception in createParquetFile",e);
        }

    }

    private void createAvroFile(Schema key, List<Object> value) {
        SpecificDatumWriter datumWriter = new SpecificDatumWriter();
        DataFileWriter dataFileWriter = new DataFileWriter(datumWriter);
        String newFilename = hdsfsOutputDirectory+File.separator + topicName + File.separator + "Event" + "_" + topicName + "_" + partitionName + "_" + sdf.format(new Date())+".avro" ;
        OutputStream os = null;

        try {
            os = getOutputStream(newFilename);
            log.info("Created data File " + newFilename);
            dataFileWriter.create(key, os);

            for (Object rec : value) {
                dataFileWriter.append(rec);
            }
        } catch (Exception e) {
            log.error("Error writing to file",e);
        } finally {
            try {
                if (dataFileWriter != null) {
                    dataFileWriter.close();
                    dataFileWriter = null;
                }
                if (os != null) {
                    os.close();
                    os = null;
                }
            } catch (IOException e) {
                log.error("Exception",e);
            }
        }
    }

    public OutputStream getOutputStream(String filePath) throws IOException {
        if(System.getenv("ECLIPSE_ENV") == null) {
            ExecutionFile executionfile = ExecutionFileFactory.create(filePath, FileOperation.OVERWRITE);
            return executionfile.openForWrite();
        } else {
            return new FileOutputStream(filePath);
        }
    }

    public Path getPathForWrite(String filePath) throws IOException {
    	ExecutionFile executionfile = ExecutionFileFactory.create(filePath, FileOperation.OVERWRITE);
        return executionfile.getPath();

    }

    private void writeOffsetFile(long offset) {
        // write success file
        String offsetFile = hdsfsOutputDirectory+File.separator+topicName+partitionId + ".OFFSET";
        OutputStream os = null;
        try {
            os = this.getOutputStream(offsetFile);
        } catch (IOException e1) {
            log.error(e1.getMessage());
            e1.printStackTrace();
        }
        if (os == null) {
            log.error("failed to write the offset file " + offsetFile );
            return;
        }
        log.debug("created offset file " + offsetFile );
        PrintStream file_stream = new PrintStream(os);
        //saving the offset + 1 since we need to start processing the next record if application goes down/ partitions are
        //reassigned
        file_stream.println(offset+1);

        try {
            file_stream.close();
            os.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public void stopRunning() {
        this.running = false;
        this.go = false;
    }
}
