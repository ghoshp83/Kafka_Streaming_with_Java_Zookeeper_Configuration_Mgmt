package com.pralay.cm.managed.zookeeper;

import com.pralay.cm.ConfigurationException;
import com.pralay.cm.managed.VersionedFileSet;
import com.pralay.cm.managed.*;
import com.pralay.cm.managed.ConcurrentModificationException;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Managing filesets in zookeeper hierarchy
 * /<root>/<name>/<version>
 */
public class ZookeeperVersionedFileSet implements VersionedFileSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperVersionedFileSet.class);
    private static final int MAX_DEPTH = 5;

    private final String root;
    private final CuratorFramework curatorFramework;

    public ZookeeperVersionedFileSet(final String root, final CuratorFramework curatorFramework) {
        this.root = root.substring(0, root.endsWith("/") ? root.length()-1 : root.length());
        this.curatorFramework = curatorFramework;
        LOGGER.debug("initializing {}", ZookeeperVersionedFileSet.class.getName());
    }


    @Override
    public int getEffective(final String name) {
        try {
            SortedSet<Integer> historic = getVersions(name);
            if (historic.isEmpty())
                return NO_FILESET;
            return historic.last();
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }


    @Override
    public SortedSet<Integer> getVersions(String name) {
        String fileSetRoot = getFileSetRoot(name);
        try {
            if (curatorFramework.checkExists().forPath(fileSetRoot) == null)
                return ImmutableSortedSet.of();

            SortedSet<String> historyPaths = ImmutableSortedSet.copyOf(curatorFramework.getChildren().forPath(fileSetRoot));
            ImmutableSortedSet.Builder<Integer> retBuilder = ImmutableSortedSet.naturalOrder();
            for (String path : historyPaths) {
                String historyPath = fileSetRoot + "/" + path;
                try {
                    retBuilder.add(Integer.parseInt(path));
                } catch (NumberFormatException e) {
                    LOGGER.debug("illegal fileset {}", historyPath);
                    continue;
                }
            }
            return ImmutableSortedSet.copyOf(retBuilder.build());
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    @Override
    public void downloadFileSetAsZip(final String name, int version, OutputStream target) {
        String configPath = getFileSetVersionPath(name, version);
        try {
            SortedSet<String> subPaths = getZKFileSetItems(name, version);
            downloadChildNodesToZip(target, configPath, subPaths);
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    @Override
    public void downloadItem(String name, int version, String item, OutputStream target) {
        try {
            IOUtils.copy(
                    new ByteArrayInputStream(curatorFramework.getData().forPath(getFileSetVersionPath(name, version) + "/" + item)),
                    target
            );
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    @Override
    public int install(final String name, final int effectiveVersion, final InputStream fileSetAsZip) {
        int newFileSet = getEffective(name) + 1;
        LOGGER.debug("installing new fileset[{}]", name);
        String newFileSetRoot = getFileSetVersionPath(name, newFileSet);
        try {
            int effective = getEffective(name);
            if (effectiveVersion != effective) {
                throw new ConcurrentModificationException(name, newFileSet, effectiveVersion);
            }
            copyToZKFromZipStream(fileSetAsZip, newFileSetRoot);
            LOGGER.info("fileset[{}] - uploaded to zookeeper[{}]", name, newFileSetRoot);
            return newFileSet;
        } catch (Exception e) {
            try {
                ZKUtil.deleteRecursive(curatorFramework.getZookeeperClient().getZooKeeper(), newFileSetRoot);
            } catch (Exception e1) {
                LOGGER.error("cannot delete uncomplete configuration candidate[{}]", newFileSetRoot);
            }
            throw new ConfigurationException(e);
        }
    }

    @Override
    public Date getInstallTime(String name, int version) {
        String fileSetVersionPath = getFileSetVersionPath(name, version);
        try {
            return new Date(curatorFramework.checkExists().forPath(fileSetVersionPath).getCtime());
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    @Override
    public void delete(String name, int version) {
        String fileSetPath = getFileSetVersionPath(name, version);
        try {
            curatorFramework.delete().deletingChildrenIfNeeded().forPath(fileSetPath);
        } catch (KeeperException.NoNodeException e) {
            LOGGER.warn("fileSet[{}] - tried to delete non existent");
        } catch (Exception e) {
            throw new ConfigurationException("cannot delete path " + fileSetPath);
        }
    }

    @Override
    public SortedSet<String> getRelativePaths(String name, int version) {
        return getPaths(getFileSetVersionPath(name, version), MAX_DEPTH);
    }

    @Override
    public Lock lock(String name, boolean write) {
        return new ZookeeperLock(curatorFramework, getFileSetRoot(name), 2000, write);
    }

    private void downloadChildNodesToZip(final OutputStream target, final String root, final Iterable<String> nodeNames) throws Exception {
        try (ZipOutputStream zo = new ZipOutputStream(target)) {
            for (String subPath : nodeNames) {
                String pathToDownload = root + "/" + subPath;
                zo.putNextEntry(new ZipEntry(subPath));
                IOUtils.copy(new ByteArrayInputStream(curatorFramework.getData().forPath(pathToDownload)), zo);
            }
        }
    }



    private void copyToZKFromZipStream(final InputStream resourcesAsZip, final String newConfigRoot) {
        try {
            final File tempRoot = Files.createTempDirectory(ZookeeperVersionedFileSet.class.getName()).toFile();
            try {
                ConfigStoreUtils.copyToDir(resourcesAsZip, tempRoot);

                final Path tempRootPath = tempRoot.toPath();
                Files.walkFileTree(tempRootPath, ImmutableSet.<FileVisitOption>of(), MAX_DEPTH, new SimpleFileVisitor<Path>(){
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            if(!attrs.isDirectory()) {
                                try {
                                    String targetPath = newConfigRoot + "/" + tempRootPath.relativize(file);
                                    curatorFramework
                                            .create()
                                            .creatingParentsIfNeeded()
                                            .forPath(
                                                    targetPath,
                                                    IOUtils.toByteArray(new FileInputStream(file.toFile())
                                                    )
                                            );
                                    LOGGER.info("fileSet resource is uploaded to zookeeper[{}]", targetPath);
                                } catch (Exception e) {
                                    throw new ConfigurationException(e);
                                }

                            }
                            return FileVisitResult.CONTINUE;
                        }
                });
            } finally {
                if (tempRoot != null)
                    try {
                        FileUtils.deleteDirectory(tempRoot);
                    } catch (IOException e) {
                        LOGGER.error("cannot delete temporary diractory[{}]", tempRoot);
                    }
            }
        } catch (IOException e) {
            throw new ConfigurationException(e);
        }
    }

    private SortedSet<String> getZKFileSetItems(final String name, final int version) throws Exception {
        return getPaths(getFileSetVersionPath(name, version), MAX_DEPTH);
    }

    private SortedSet<String> getPaths(String root, int depth) {
        try {
            ImmutableSortedSet.Builder<String> retBuilder = ImmutableSortedSet.naturalOrder();
            for (String item : curatorFramework.getChildren().forPath(root)) {
                SortedSet<String> children = getPaths(root + "/" + item, depth - 1);
                if (children.isEmpty()) {
                    retBuilder.add(item);
                } else {
                    for (String subPath : children) {
                        retBuilder.add(item + "/" + subPath);
                    }
                }
            }
            return retBuilder.build();
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    private String getFileSetVersionPath(final String name, final int version) {
        return getFileSetRoot(name) + "/" + version;
    }

    private String getFileSetRoot(final String name) {
        if (name == null)
            return root;
        int startIndex = 0;
        while (name.charAt(startIndex) == '/')
            startIndex++;
        return root + "/" + name.substring(startIndex, name.endsWith("/") ? name.length() - 1 : name.length());
    }
}
