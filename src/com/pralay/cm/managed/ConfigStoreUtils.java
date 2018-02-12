package com.pralay.cm.managed;

import com.pralay.cm.CMModule;
import com.pralay.cm.ConfigStoreRead;
import com.pralay.cm.ConfigurationException;
import com.pralay.cm.VersionedFileSetRead;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class ConfigStoreUtils {
    public static void copyToDir(InputStream zipStream, File root) throws IOException {
        ZipInputStream zi = new ZipInputStream(zipStream);
        ZipEntry ze = zi.getNextEntry();
        while (ze != null) {
            File targetFile = new File(root, ze.getName());
            if (!targetFile.getParentFile().exists())
                targetFile.getParentFile().mkdirs();
            IOUtils.copy(zi, new FileOutputStream(targetFile));
            ze = zi.getNextEntry();
        }
    }


    public static InputStream zipDirToStream(String root) throws IOException {
        return zipDirToStream(root, getChildrenRelative(Paths.get(root)));
    }

    public static InputStream zipDirToStream(String root, Iterable<String> filesRelative) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try (ZipOutputStream zout = new ZipOutputStream(bout)) {
            for (String fileRelative: filesRelative) {
                ZipEntry ze = new ZipEntry(fileRelative);
                zout.putNextEntry(ze);
                IOUtils.copy(new FileInputStream(new File(root, fileRelative)), zout);
                zout.closeEntry();
            }
        }
        return new ByteArrayInputStream(bout.toByteArray());
    }


    public static Path downloadToTemp(ConfigStoreRead configStoreRead, String appsuite, int version) {
        if (!StringUtils.isBlank(appsuite)) {
            int effective = configStoreRead.getEffective(appsuite);
            try {
                Path tempDir = Files.createTempDirectory(CMModule.class.getSimpleName());
                if (effective != VersionedFileSetRead.NO_FILESET) {
                    File tempZip = Files.createTempFile(CMModule.class.getSimpleName(), ".zip").toFile();
                    try (OutputStream out = new FileOutputStream(tempZip)){
                        configStoreRead.downloadFileSetAsZip(appsuite, version, out);
                    }
                    copyToDir(new FileInputStream(tempZip), tempDir.toFile());
                    tempZip.delete();
                    return tempDir;
                } else
                    throw new ConfigurationException("no config to download");
            } catch (IOException e) {
                throw new ConfigurationException(e);
            }
        } else
            throw new ConfigurationException("no appsuite provided");
    }

    public static void copyToZip(Path sourceDir, Path targetFile) throws IOException {
        IOUtils.copy(zipDirToStream(sourceDir.toString(), getChildrenRelative(sourceDir)), new FileOutputStream(targetFile.toFile()));
    }

    public static ImmutableSet<String> getChildrenRelative(final Path root) throws IOException {
        final ImmutableSet.Builder<String> ret = ImmutableSet.builder();
        java.nio.file.Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (!attrs.isDirectory())
                    ret.add(root.relativize(file).toString());
                return FileVisitResult.CONTINUE;
            }
        });
        return ret.build();
    }
}
