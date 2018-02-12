package com.pralay.bigdata.zk;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pralay.core.executionfile.ExecutionFile;
import com.pralay.core.executionfile.ExecutionFileFactory;
import com.pralay.core.executionfile.FileOperation;

public class ZkUtils {
    private static final Logger log = LoggerFactory
            .getLogger(ZkUtils.class.getName());
    public final static String HDFS_PREFIX = "hdfs://";
    public static boolean isHdfs(String path) {
        if (path == null || path.isEmpty())
            return false;
        if (path.startsWith(HDFS_PREFIX))
            return true;
        return false;
    }

    public static String getActualPath(String path) {
        if (isHdfs(path)) {
            return path.replace(HDFS_PREFIX, "");
        }
        return path;
    }

    public static String getAbsolutePath(String path) {
        String p = getActualPath(path);
        if (!isHdfs(path)) {
            File f = new File(path);
            p = f.getAbsolutePath();
        }
        return p;
    }

    public static OutputStream getOutPutStream(String filePath) {
        log.info("About to get output stream for file: " + filePath);
        OutputStream os = null;
        try {
            if (!isHdfs(filePath))  {
                // create folders in path
                File targetFile = new File(filePath);
                File parent = targetFile.getParentFile();

                if (parent != null) {
                    if (!parent.exists() && !parent.mkdirs()) {
                         throw new IllegalStateException("Couldn't create dir: " + parent);
                    }
                }
                os = new FileOutputStream(filePath, false);

            } else {
                log.info("About to get output string from hdfs for file: " + filePath);
                ExecutionFile executionfile = ExecutionFileFactory.create(getActualPath(filePath), FileOperation.OVERWRITE);
                os = executionfile.openForWrite();
            }
        } catch (Exception e) {
            log.error("failed to get Outputstream :" + e.getMessage());
        }
        if (os == null)
            log.error("failed to get Outputstream for file: " + filePath);
        return os;

    }

    public static InputStream getInputStream(String filePath) throws IOException {

        if (!isHdfs(filePath)) {
            return new FileInputStream(filePath);
        } else {

        	ExecutionFile executionfile = ExecutionFileFactory.create(getActualPath(filePath), FileOperation.READ);
            return executionfile.openForRead();
        }
    }

}
