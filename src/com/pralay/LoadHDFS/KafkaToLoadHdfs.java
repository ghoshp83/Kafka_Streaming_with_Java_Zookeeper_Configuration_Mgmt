package com.pralay.LoadHDFS;

/*
This is the main class that creates the AdapterConsumerThread instances and monitors the running threads
Assumption is that we will have one consumer to handle every partition - only if one of the instances go down,
a single consumer instance will process more than one partition - This is managed by Kafka as long as all consumers
for a particular topic have the same consumer group id
 */

import com.pralay.core.ExecutionContext;
import com.pralay.core.executionfile.ExecutionFile;
import com.pralay.core.executionfile.ExecutionFileFactory;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;

import com.pralay.core.executionfile.FileOperation;
import com.pralay.core.configuration.ServerAddr;
import com.pralay.bigdata.zk.ZkDataService;
import com.pralay.LoadHDFS.kafka.InitTopic;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaToLoadHdfs {
    private static Logger log ;
    public static boolean stopMonitoring = false;
    public static SimpleConfig config = null;
    public static String configFileName = null;
    public static String topic = null;
    public static List<Thread> listOfRunThreads = new ArrayList();
    public static HashMap<String,Schema> schemaHashMap = new HashMap<String,Schema>();
    public static String zookeeperConnectString = "localhost:2181"; //default for eclipse_env
    private static String hdfsPath;
    public static ZkDataService ds;
    private static String zkNameSpace = "TestAPP";
    public static final String LOG_CONFIGURATION_FILE = "log4j.xml";

    public static ExecutionContext context;
    public static boolean isUnitTest = false;
    public static FileSystem fs;
    public static KafkaConsumer mockConsumer;
    public static ExecutionFile executionfile;
    public static String appVersion;

    public KafkaToLoadHdfs() {

    }

    public static boolean isStopMonitoring() {
        return stopMonitoring;
    }

    public static synchronized void setStopMonitoring(boolean stopMonitoring1) {
        stopMonitoring = stopMonitoring1;
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        try {
            //Setup the log first - so it can be used subsequently
            LogConfigurator.configure(LOG_CONFIGURATION_FILE);
            log = LoggerFactory.getLogger(KafkaToLoadHdfs.class.getName());

            StopProcess shutdownHook = new StopProcess();

            if (System.getProperty("ECLIPSE_ENV") == null) {
                int maxPriority = Integer.MAX_VALUE; // Execute application hook before hook in Hadoop library
                ShutdownHookManager.get().addShutdownHook(shutdownHook, maxPriority);
                try {
                    if(!isUnitTest) {
                        fs = ExecutionFileFactory.getHdfsFileSystem();
                    }
                } catch (IOException var8) {
                    log.error("failed to initialize HDFS File System :" + var8.getMessage());
                    var8.printStackTrace();
                }
                //Zookeeper connection string
                // get the zookeeperString from the Env
                // create context with default configuration from environment
                if(!isUnitTest) {
                    context = ExecutionContext.create();
                }
                // get zookeeper host list from Env context
                List<ServerAddr> zookeeperList = context.getConfiguration().getZookeeperHosts();
                zookeeperConnectString = getConnectionStr(zookeeperList);
            } else {
                Runtime.getRuntime().addShutdownHook(shutdownHook);
            }

            // process command line options
            CommandOptions processOptions = new CommandOptions(args);
            if (!processOptions.processOptions()) {
                System.out.println("invalid Program options, exiting");
                log.error("invalid Program options, exiting");
                System.exit(-1);
            }
            if (!isUnitTest) {
                if (configFileName == null || configFileName.isEmpty()) {
                    Properties prop = new Properties();
                    FileInputStream is = new FileInputStream(new File("config.txt"));
                    prop.load(is);
                    config = new SimpleConfig(prop);
                } else {
                    config = new SimpleConfig(configFileName);
                }
            }
            if (System.getProperty("ECLIPSE_ENV") == null) {
                //Set the HDFS path to the Schema files
                String appVersionStr = processOptions.getAppVersionOption();
                log.info("Starting ..., appVersion: " + appVersionStr);
                hdfsPath = String.format("hdfs:///test_apps/KafkaToLoadHdfs-%s/", System.getProperty("user.name"), appVersionStr);

            }
            if(!isUnitTest) {
                ds = new ZkDataService(zookeeperConnectString, zkNameSpace);
            }
            if(!isUnitTest) {
                initSchemaMap();
            } else {
                loadTheSchemasFromFileSystem();
            }
            if (schemaHashMap.isEmpty()) {
                log.error("No schema(s) exists for HdfsLoader in zoo keeper - exiting the Application");
                System.exit(-1);
            }

            long delay = 60000L;
            String log4jConfigFilename = "log4j.xml";
            log.info("Setting a log4j Watch, for " + log4jConfigFilename + " delay =" + delay);

            try {
                DOMConfigurator.configureAndWatch(log4jConfigFilename, delay);
            } catch (Exception var7) {
                log.error("could not set configureAndwatch for log4j.xml : " + var7.getMessage());
            }


            //start a thread for each topic
            String topicNames = KafkaToLoadHdfs.config.adapterKafkaTopicNames();
            //If topic names have been passed in the command line, use that
            if (topic != null && !topic.isEmpty()) {
                topicNames = topic;
            }

            if (topicNames == null || topicNames.trim().isEmpty()) {
                log.error("No Topic names specified, exiting");
                System.exit(-1);

            }

            String[] topicsArray = topicNames.split(",");
            for(String topic : topicsArray) {
                if(!topic.trim().isEmpty()) {
                    startConsumerThread(topic.trim());
                }
            }
            if(!isUnitTest) {
                monitorRunningThreads();
            }
        }catch (Exception e) {
            log.error("Exception in main",e);
            e.printStackTrace();
        } catch (Throwable e) {
            log.error("Exception in main",e);
            e.printStackTrace();
        }
    }

    private static void startConsumerThread(String topicName) {
        try {
            log.info("Starting consumer thread for topic:"+topicName);
            AdapterConsumerThread consumerT = new AdapterConsumerThread(topicName);
            consumerT.setUnitTest(isUnitTest);
            if(isUnitTest) {
                consumerT.setConsumer(mockConsumer);
            }
            listOfRunThreads.add(consumerT);
            consumerT.start();
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
            log.error("Exception starting consumer",e);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Exception starting consumer",e);
        }
    }


    public static void monitorRunningThreads() {
        if (listOfRunThreads == null || listOfRunThreads.isEmpty())
            return;

        log.info("Started monitoring running Threads: " + listOfRunThreads);
        List<Thread> deadThreads = new ArrayList<Thread>();
        while (!isStopMonitoring()) {
            Iterator<Thread> it = listOfRunThreads.iterator();
            if (!deadThreads.isEmpty())
                deadThreads.clear();

            while (it.hasNext()) {
                try {
                    Thread t = it.next();
                    // java.lang.Thread.State can be NEW, RUNNABLE, BLOCKED,
                    // WAITING, TIMED_WAITING, TERMINATED
                    log.info(t.getName() + " state:" + t.getState());
                    if (!t.isAlive()) {
                        log.error(t.getName() + " is not alive");
                        deadThreads.add(t);
                        it.remove();
                    }

                } catch (Exception e) {
                    log.error("Unexpected exception", e);
                }
            }

            // Iterate through list of stopped threads and restart them
            for ( Thread t: deadThreads) {
                try {
                    log.info(t.getName() + " thread is being started again");
                    if(t instanceof AdapterConsumerThread) {
                        AdapterConsumerThread act = (AdapterConsumerThread) t;
                        startConsumerThread(act.getTopicName());
                    }

                } catch (Exception e) {
                    log.debug("Thread exception: ", e);
                }
            }

            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);

            }
        }

    }

    public static void stopRunningThreads() {
        if(listOfRunThreads != null && !listOfRunThreads.isEmpty()) {
            log.info("stopping all running threads");
            ArrayList deadThreads = new ArrayList();

            Iterator it = listOfRunThreads.iterator();
            if (!deadThreads.isEmpty()) {
                deadThreads.clear();
            }

            while (it.hasNext()) {
                try {
                    Thread e = (Thread) it.next();
                    log.info(e.getName() + " state:" + e.getState());
                    if (e.isAlive() && e instanceof AdapterConsumerThread) {
                        ((AdapterConsumerThread) e).stopRunning();
                    }

                    it.remove();
                } catch (Exception e) {
                    log.error("Unexpected exception", e);
                }
            }
        }
    }

    static class StopProcess extends Thread {
        StopProcess() {
        }

        public void run() {
            System.out.println("Stopping process...");
            KafkaToLoadHdfs.log.info("Stopping process...");
            KafkaToLoadHdfs.stopMonitoring = true;
            KafkaToLoadHdfs.stopRunningThreads();
            KafkaToLoadHdfs.log.info("Exiting...");
        }
    }

    private static void initSchemaMap() {
        try {
            log.info("in initSchemaMap");
            List<String> childPathList = new ArrayList<String>();
            getChildPaths(childPathList,InitTopic.DEST_PREFIX+InitTopic.ZK_SCHEMA_NODE,ds);

            for (String childPath : childPathList) {

                String schemaStr = ds.get(childPath);
                if(schemaStr != null) {
                    ds.put(schemaStr, childPath);
                }

                Schema.Parser parser = new Schema.Parser();
                //Omit the json file name of the schema when loading into hash map
                int idx = childPath.lastIndexOf("/");
                String schemaKey = childPath.substring(0,idx);
                schemaHashMap.put(schemaKey.toUpperCase(), parser.parse(schemaStr));
            }

        } catch (Exception e) {
            log.error("Exception in initSchema",e);
        } catch (Throwable e) {
            log.error("Exception in initSchema",e);
        }
    }

    protected static void loadTheSchemasFromFileSystem() {
        try {
            String fName;

            if(System.getProperty("ECLIPSE_ENV") != null) {
                fName = "./Schemas";
            } else {
                fName = hdfsPath+"Schemas";
            }
            List<String> files = getFileList(fName);
            for(String fileName : files) {
                int startIdx = fileName.indexOf("Schemas");
                int endIdx = fileName.lastIndexOf(File.separatorChar);
                String zkPath = fileName.substring(startIdx+8,endIdx == -1 ? fileName.length() : endIdx);
                zkPath = zkPath.replaceAll(Matcher.quoteReplacement(File.separator),"/");
                zkPath = InitTopic.DEST_PREFIX + InitTopic.ZK_SCHEMA_NODE + "/" + zkPath;

                InputStream fis = getInputStream(fileName);
                InputStreamReader isr = new InputStreamReader(fis);
                BufferedReader reader = new BufferedReader(isr);
                String line = "";
                StringBuffer entireSchema = new StringBuffer();

                while ((line = reader.readLine()) != null) {
                    entireSchema.append(line);
                }
                fis.close();
                isr.close();
                reader.close();

                zkPath = zkPath.toUpperCase();

                String schemaStr = entireSchema.toString();

                Schema.Parser parser = new Schema.Parser();
                schemaHashMap.put(zkPath, parser.parse(schemaStr));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param zookeeperList
     *            : list of zookeeper hosts
     * @return a connection string of the format
     *         "hostname1:clientPortNum1, hostname2:clientPortNum2..."
     */
    public static String getConnectionStr(List<ServerAddr> zookeeperList) {
        if (zookeeperList == null || zookeeperList.isEmpty())
            return "localhost:2181";
        StringBuffer connectStr = new StringBuffer();
        for (ServerAddr s : zookeeperList) {
            if (connectStr.length() > 0)
                connectStr.append(",");

            connectStr.append(s.getHost());
            connectStr.append(":");
            connectStr.append(s.getPort());
        }
        log.info("connectStr:" + connectStr.toString());
        return connectStr.toString();
    }

    public static InputStream getInputStream(String filePath) throws IOException {
        if(System.getProperty("ECLIPSE_ENV") != null) {
            return new FileInputStream(filePath);
        } else {
            if(!isUnitTest) {
                executionfile = ExecutionFileFactory.create(filePath, FileOperation.READ);
                return executionfile.openForRead();
            }
            else {
                return executionfile.openForRead().getWrappedStream();
            }
        }
    }


    public static List<String> getFileList(String path) {
        ArrayList list = new ArrayList();
        if(System.getProperty("ECLIPSE_ENV") == null) {
            updateFileList((Path)(new Path(path)), list);
        } else {
            updateFileList((File)(new File(path)), list);
        }

        return list;
    }

    public static void updateFileList(Path path, List<String> list) {
        try {

            FileStatus[] stats = fs.listStatus(path);
            if(stats != null) {
                int len = stats.length;

                for(int i = 0; i < len; ++i) {
                    FileStatus stat = stats[i];
                    if(stat.isDir()) {
                        FileStatus[] dataFile = fs.listStatus(stat.getPath());
                        if(dataFile != null && dataFile.length > 0) {
                            updateFileList(stat.getPath(), list);
                        }
                    } else {
                        String fileName = stat.getPath().toString();
                        list.add(fileName);
                        log.info("new file added to the file list " + fileName);
                    }
                }
            }
        } catch (IOException e) {
            log.error("IOException updateFileList() " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e1) {
            log.error("Exception updateFileList() " + e1.getMessage());
            e1.printStackTrace();
        }

    }

    public static void updateFileList(File path, List<String> list) {
        try {
            File[] filesArr = path.listFiles();
            if(filesArr != null) {
                int len = filesArr.length;

                for(int i = 0; i < len; ++i) {
                    File stat = filesArr[i];
                    if(stat.isDirectory()) {
                        File[] dataFile = stat.listFiles();
                        if(dataFile != null && dataFile.length > 0) {
                            updateFileList(stat, list);
                        }
                    } else if(stat.isFile()) {
                        String fileName = stat.getPath().toString();
                        list.add(fileName);
                        log.info("New file added to the file list " + fileName);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Exception: " + e.getMessage(),e);
        }

    }

    /**
     * Used by unit test
     */
    public static void setupLog() {
        //Setup the log first - so it can be used subsequently
        LogConfigurator.configure(LOG_CONFIGURATION_FILE);
        log = LoggerFactory.getLogger(KafkaToLoadHdfs.class.getName());
    }

    /**
     * Returns the full path to the leaf nodes under the startPath
     * @param childPathList
     * @param startPath
     * @param zkDs
     */
    public static void getChildPaths(List<String> childPathList, String startPath, ZkDataService zkDs) {
        try {
            List<String> children = zkDs.getChildrenFullyQualified(startPath);
            if(children != null && !children.isEmpty()) {
                for(String childPath : children) {
                    getChildPaths(childPathList, childPath,zkDs);
                }
            } else {
                childPathList.add(startPath);
            }
        } catch (Exception e) {
            log.error("Error in getChildPaths",e);
        }
    }
}
