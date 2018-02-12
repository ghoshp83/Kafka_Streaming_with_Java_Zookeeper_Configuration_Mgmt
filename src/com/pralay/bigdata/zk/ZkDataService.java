package com.pralay.bigdata.zk;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorTempFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pralay.core.executionfile.ExecutionFileFactory;

/**
 * First version of Zookeeper based data service
 * Can be used to store config params and contents of file
 * Note that no exceptions are handled in the code and everything is thrown out, to be handled by the using Application appropriately
 * Features:
 * 1. Instantiate a ZkDataService object by giving a ZK connect string and a namespace (to differentiate the storage area inside ZK for different usecases)
 * 2. Get and Put config params
 * 3. Get and Put contents from local file system into ZK and vice versa
 * 4. Support recursive upload from local file preserving the local from directory structure under a zookeeper node.
 * 5. Support recursive download from zookeper from to local file preserving zookeeper directory structure.
 *
 */
public class ZkDataService {
    private static final Logger log = LoggerFactory
            .getLogger(ZkDataService.class.getName());
    private CuratorFramework cf;

    public CuratorFramework getCf() {
        return cf;
    }

    private String nameSpace;


    /** Get contents of specified remotePath as String
     * @param remotePath: remotePath of the object in ZK
     * @return Contents of specified remotePath as String
     * @throws Exception
     */
    public String get(String remotePath) throws Exception {

        if (cf.checkExists().forPath(remotePath) == null)
            return null;

        byte[] data = cf.getData().forPath(remotePath);
        if(data==null)
            return null;
        else
            return new String(data);
    }

    /** Get contents of specified remotePath as String
     * @param remotePath: remotePath of the object in ZK
     * @return Contents of specified remotePath as byte[]
     * @throws Exception
     */
    public byte[] getBytes(String remotePath) throws Exception {

        if (cf.checkExists().forPath(remotePath) == null)
            return null;

        byte[] data = cf.getData().forPath(remotePath);
        if(data==null)
            return null;
        else
            return data;
    }

    /**
     * @param remotePath
     * @return return the content as a Property file
     * @throws Exception
     */
    public Properties getPropertiesFromZK(String remotePath) throws Exception {
        String content = get(remotePath);
        Properties p = new Properties();
        InputStream is = new ByteArrayInputStream(content.getBytes());
        p.load(is);
        return p;
    }

    /**
     * @param remotePath
     * @return return the content of Zookeper remotePath as a InputStream
     * @throws Exception
     */
    public InputStream getInputStreamFromZK(String remotePath) throws Exception {
        byte[] content = getBytes(remotePath);
        log.info("remotePath: " + remotePath + "byte length: " + content.length);
        InputStream is = (content == null) ? null : new ByteArrayInputStream(content);
        return is;
    }

    /** Get contents of specified remotePath into a local file with given path
     * @param remotePath: remotePath of the object in ZK
     * @param localFilePath: local file system path to which the contents of remote object needs to be copied
     * @return void
     * @throws Exception
     */
    public void getContentsIntoFile(String remotePath, String localFilePath) throws Exception
    {
        byte[] bytes = getBytes(remotePath);
        if (bytes == null) {
            bytes = new byte[0];
        }
        if (!ZkUtils.isHdfs(localFilePath))
           FileUtils.writeByteArrayToFile(new File(localFilePath), bytes);
        else {
            // write to Hdfs
            OutputStream os = ZkUtils.getOutPutStream(localFilePath);
            PrintStream file_stream = new PrintStream(os);
            file_stream.write(bytes);
            file_stream.flush();
            file_stream.close();
        }
    }

    /**
     * @param remotePath: can be a dir or file.
     * @throws Exception
     */
    public void getRecursiveContentsIntoFile(String remotePath, String dest) throws Exception
    {
         List <String> l = cf.getChildren().forPath(remotePath);
        //String content = get(remotePath);
        if (!l.isEmpty()) {
            Iterator <String> it  = l.iterator();
            while (it.hasNext()) {
                getRecursiveContentsIntoFile(remotePath+"/"+it.next(), dest);
            }

        }
        else  {
            String finalDestination = dest + "/" + remotePath;
            getContentsIntoFile(remotePath,finalDestination);
        }

    }

    /**
     * @param remotePath
     * @return List of children nodes
     * @throws Exception
     */
    public List <String> getChildren(String remotePath) throws Exception {
        List <String> l = cf.getChildren().forPath(remotePath);
        return l;
    }

    /**
     * @param remotePath
     * @return fully qualified list of children nodes : i.e remotePath/childnode
     * @throws Exception
     */
    public List <String> getChildrenFullyQualified(String remotePath) throws Exception {
        List <String> l2 = new ArrayList<String>();
        if (cf.checkExists().forPath(remotePath) == null)
            return l2; //empty list

        List <String> l = cf.getChildren().forPath(remotePath);
        if (l == null || l.isEmpty())
            return l2;

        Iterator <String> it = l.iterator();

        while (it.hasNext()) {
            l2.add(remotePath + "/" + it.next());
        }
        return l2;
    }

    /** Put specified String content into specified remotePath as String
     * @param content: String content to be stored
     * @param remotePath: remotePath of the object in ZK
     * @return void
     * @throws Exception
     */
    public void put(String content, String remotePath) throws Exception
    {

        if (cf.checkExists().forPath(remotePath) != null)
             cf.setData().forPath(remotePath, content.getBytes());
        else {
            cf.create().creatingParentsIfNeeded().forPath(remotePath, content.getBytes());
        }
    }

    /** Put specified String content into specified remotePath as String Ephemeral
     * The string will disappears once the connection ends.
     * @param content: String content to be stored
     * @param remotePath: remotePath of the object in ZK
     * @return void
     * @throws Exception
     */
    public void putEphemeral(String content, String remotePath) throws Exception
    {

        if (cf.checkExists().forPath(remotePath) != null)
             cf.setData().forPath(remotePath, content.getBytes());
        else {
            cf.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(remotePath, content.getBytes());
        }
    }

    public void putBytes(byte[] content, String remotePath) throws Exception
    {

        if (cf.checkExists().forPath(remotePath) != null)
             cf.setData().forPath(remotePath, content);
        else {
            cf.create().creatingParentsIfNeeded().forPath(remotePath, content);
        }
    }

    public boolean exist(String remotePath) throws Exception
    {

        if (cf.checkExists().forPath(remotePath) != null)
            return true;
        else
            return false;
    }

    /** Put contents of specified local file into into specified remotePath as String
     * @param localFilePath: path for the file in local file system
     * @param remotePath: remotePath of the object in ZK
     * @throws Exception
     */
    public void putContentsFromFile(String localFilePath, String remotePath) throws Exception
    {
        byte[] bytes = null;
        if (!ZkUtils.isHdfs(localFilePath))
            bytes = FileUtils.readFileToByteArray(new File(localFilePath));
        else {
            // get the content from hdfs.
            InputStream is = ZkUtils.getInputStream(localFilePath);
            bytes = IOUtils.toByteArray(is);
        }
        if (bytes != null) {
            putBytes(bytes, remotePath);
            log.info("copied " + localFilePath + " to " + remotePath + "; " + bytes.length + "bytes");
        }
    }

    public void putContentsFromHadoopPath(String localFilePath, String remotePath) throws Exception
    {
        byte[] bytes = null;
        if (!ZkUtils.isHdfs(localFilePath))
            bytes = FileUtils.readFileToByteArray(new File(localFilePath));
        else {
            // get the content from hdfs.
            InputStream is = ZkUtils.getInputStream(localFilePath);
            bytes = IOUtils.toByteArray(is);
        }
        if (bytes != null)
            putBytes(bytes, remotePath);
    }
    public int putContentsFromInputStream(InputStream is, String remotePath) throws Exception {
        byte[] bytes = IOUtils.toByteArray(is);
        if (bytes != null)
            putBytes(bytes, remotePath);
        return bytes.length;
    }

    /**Copies the content of file or directory to a destination.
     * if the source is a directory it will recursively copy the content of the sorce directory
     * preserving the same directory structure into zookeeper.
     * @param localFilePath
     * @param dest
     * @throws Exception
     */
    public void putRecursiveContentsFromFile(String localFilePath, String dest) throws Exception
    {
        if (!ZkUtils.isHdfs(localFilePath)) {
            log.info("Processing from local file system!!!!!!!");
            putRecursiveContentsFromRegularFile(localFilePath, dest);
        }
        else {
            log.info("Processing from Hadoop file system!!!!!!!");
            FileSystem fs =  ExecutionFileFactory.getHdfsFileSystem();
            log.info("fs.default.name : - " + fs.getConf().get("fs.default.name"));
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(ZkUtils.getActualPath(localFilePath));
            putRecursiveContentsFromHdfsFile(path, dest, ZkUtils.getActualPath(localFilePath));
        }

    }

    public void putRecursiveContentsFromRegularFile(String localFilePath, String dest) throws Exception
    {
        File f = new File(localFilePath);
        if (f.isFile())
            this.putContentsFromFile(localFilePath, dest);
        else
            this.putRecursiveContentsFromFile(localFilePath, localFilePath, dest);

    }

    /**
     * @param path
     * @param dest
     * @param startNode
     * @throws Exception
     */
    public void putRecursiveContentsFromHdfsFile(org.apache.hadoop.fs.Path path, String dest, String startNode) throws Exception {
        final FileSystem fileSytem = ExecutionFileFactory.getHdfsFileSystem();
        FileStatus[] stats = fileSytem.listStatus(path);
        if (stats != null) {
            for (FileStatus stat : stats) {
                if (stat.isDirectory()) {
                    putRecursiveContentsFromHdfsFile(stat.getPath(), dest, startNode);
                } else if (stat.isFile()) {
                    FSDataInputStream is = fileSytem.open(stat.getPath());
                    String uriStr = stat.getPath().toUri().toString();
                    int index = uriStr.indexOf(startNode);
                   String remotePath = uriStr.substring(index + startNode.length());
                   remotePath = dest + remotePath;
                    log.info("about to process hdfs file: " + uriStr + "to remote path" + remotePath);
                    int bytes = putContentsFromInputStream(is, remotePath);
                   if (bytes > 0)
                       log.info("copied " + uriStr + " to " + remotePath + "; " + bytes + "bytes");
                }
            }
        }
    }


    /**
     * @param localFilePath
     * @param startNode
     * @param dest
     * @throws Exception
     */
    private void putRecursiveContentsFromFile(String localFilePath, String startNode, String dest) throws Exception
    {
        File dir = new File(localFilePath);
        String[] files = dir.list();
        if (files!=null && files.length > 0) {
            for (String f : files) {
                putRecursiveContentsFromFile(localFilePath + "/" + f, startNode, dest);
            }
        }
        else {
            int index = localFilePath.lastIndexOf(startNode);
            if(index >= 0) {
                String remotePath = dest + localFilePath.substring(index + startNode.length());
                putContentsFromFile(localFilePath,remotePath);
            }
        }

    }

    /**
     * @param path: path to be deleted this method deletes any children nodes as well
     * @throws Exception
     */
    public void delete(String path) throws Exception {

        if (cf.checkExists().forPath(path) != null) {
            cf.delete().deletingChildrenIfNeeded().forPath(path);
            log.info("deleted: " + path);
        }

    }

    public String getNameSpace()
    {
        return nameSpace;
    }
    /**
     * Stop the client
     */
    public void close() {
        cf.close();

    }


    /**
     * @param timeOut: connection time out time in miliseconds
     * @param zkConnectionString: ZK quorum connection string of the format "hostname1:clientPortNum1, hostname2:clientPortNum2..."
     * @param nameSpace: Separate nameSpace to be used within ZK store for this client, to be used to differentiate data stored for different usecases
     */
    public ZkDataService(String zkConnectionString, String nameSpace, Integer timeOut) {
        this.nameSpace = nameSpace;
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);

        // The simplest way to get a CuratorFramework instance. This will use default values.
        // The only required arguments are the connection string and the retry policy
        Integer timeOutWait = 60000;
        if (timeOut != null)
            timeOutWait = timeOut;
        // The simplest way to get a CuratorFramework instance. This will use default values.
        // The only required arguments are the connection string and the retry policy
        cf = CuratorFrameworkFactory.builder().connectString(zkConnectionString).namespace(nameSpace).retryPolicy(retryPolicy).connectionTimeoutMs(timeOutWait).build();
        cf.start();
    }

    /**
     * @param zkConnectionString: ZK quorum connection string of the format "hostname1:clientPortNum1, hostname2:clientPortNum2..."
     * @param nameSpace: Separate nameSpace to be used within ZK store for this client, to be used to differentiate data stored for different usecases
     */
    public ZkDataService(String zkConnectionString, String nameSpace) {
        this.nameSpace = nameSpace;
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);

        // The simplest way to get a CuratorFramework instance. This will use default values.
        // The only required arguments are the connection string and the retry policy
        // The simplest way to get a CuratorFramework instance. This will use default values.
        // The only required arguments are the connection string and the retry policy
        cf = CuratorFrameworkFactory.builder().connectString(zkConnectionString).namespace(nameSpace).retryPolicy(retryPolicy).build();
        cf.start();
    }

    //========================================================================================================================
    /**
     * The following are for read-only CuratorTempFramework. It is a light weight version of ZooKeeper getData only client service.
     * It will only open a getData thread as on-demand basis. The thread will automatically close after 60 seconds of no activities.
     * After the thread closed, it would be opened again once new demand showed up.
     *
     */
    private static CuratorTempFramework ctf = null;
    public  static boolean initZKdataReadonly(String zkConnectionString, String nameSpace) {
        if (ctf == null) {
            ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
            ctf= CuratorFrameworkFactory.builder().connectString(zkConnectionString).namespace(nameSpace)
                    .retryPolicy(retryPolicy).buildTemp(60, TimeUnit.SECONDS);
        }
        if ( ctf == null) return false;
        else              return true;
    }
    /**
     * Get data (byte{}) from ZooKeeper with the remotePath (assumed anchored under /APP)
     * The initZKdataReadonly(...) must run once, before using this get service.
     * @param remotePath e.g. test/test.properties (i.e. /APP/test/test.properties)
     * @return
     * @throws Exception
     */
    public static byte[] getZKdataReadonly(String remotePath) throws Exception  {
        byte[] data = ctf.getData().forPath(remotePath);
        if(data==null)
            return null;
        else
            return data;
    }
    /*
     * no need to close, it will close automatically if no activities.
     */
    public  static void closeZKdataReadonly() {
        if (ctf != null)
            ctf.close();
    }


    /**
     * @param path
     *            parent path
     * @param watcher
     *            Watcher to receive notification
     * @return a list of children being watched.
     * @throws Exception
     */
    public List<String> watchedGetChildren(String path, CuratorWatcher watcher)
            throws Exception {
        /**
         * Get children and set the given watcher on the node.
         */
        return cf.getChildren().usingWatcher(watcher).forPath(path);

    }


}
