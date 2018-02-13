package com.pralay.core.executionfile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutionFileFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionFileFactory.class);

    private static Configuration hdfsConf;
    private static FileSystem hdfsFileSystem = null;

    static {
        hdfsConf = new Configuration();
        hdfsConf.addResource(new Path("core-site.xml"));
        hdfsConf.addResource(new Path("hdfs-site.xml"));

        try {
            hdfsFileSystem = FileSystem.get(hdfsConf);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public static ExecutionFile create(String pathString, FileOperation operation) {

        return new ExecutionFile(hdfsFileSystem, pathString, operation);
    }

    public static FileSystem getHdfsFileSystem() throws IOException {

        return hdfsFileSystem;
    }
}
