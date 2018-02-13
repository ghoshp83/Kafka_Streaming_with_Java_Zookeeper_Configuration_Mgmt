package com.pralay.cm.managed.zookeeper;

import com.pralay.cm.ConfigurationException;
import com.pralay.cm.managed.VersionedFileSet;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ZookeeperLock implements VersionedFileSet.Lock {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperLock.class);

    public static final int DEFAULT_ACQUIRE_TIMEOUT_MILLIS = 2000;

    private InterProcessMutex lock = null;
    private final String path;
    private final static ThreadLocal<Boolean> lockStatus = new ThreadLocal<>();

    static ZookeeperLock write(CuratorFramework curator, String path) {
        return write(curator, path, DEFAULT_ACQUIRE_TIMEOUT_MILLIS);
    }

    static ZookeeperLock write(CuratorFramework curator, String path, long acquireTimeout) {
        return new ZookeeperLock(curator, path, acquireTimeout, true);
    }

    static ZookeeperLock read(CuratorFramework curator, String path) {
        return read(curator, path, 2000);
    }

    static ZookeeperLock read(CuratorFramework curator, String path, long acquireTimeout) {
        return new ZookeeperLock(curator, path, acquireTimeout, false);
    }

    ZookeeperLock(CuratorFramework curator, String path, long acquireMs, boolean write) {
        InterProcessReadWriteLock readWriteLock = new InterProcessReadWriteLock(curator, path);
        this.path = path;
        if (lockStatus.get() != Boolean.TRUE) {
            lock = write ? readWriteLock.writeLock() : readWriteLock.readLock();
            try {
                if (!lock.acquire(acquireMs, TimeUnit.MILLISECONDS)) {
                    LOGGER.error("ZKLock for path[{}] {} cannot acquire!", path, write ? "write" : "read");
                    throw new ConfigurationException("cannot acquire lock path [" + path + "]");
                }
                LOGGER.debug("ZKLock for path[{}] {}", path, write ? "write" : "read");
                lockStatus.set(write);
            } catch (ConfigurationException e) {
                throw e;
            } catch (Exception e) {
                throw new ConfigurationException("cannot acquire lock path [" + path + "]", e);
            }
        } else {
            LOGGER.debug("already write locked in the same thread");
        }
    }

    @Override
    public void close() {
        try {
            if (lock != null) {
                lock.release();
                LOGGER.debug("ZKLock for path[{}] released", path);
                lockStatus.set(null);
            }
        } catch (Exception e) {
            LOGGER.error("ZKLock for path[" + path + "] cannot close lock properly", e);
        }
    }
}
