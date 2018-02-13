package com.pralay.cm;

import java.io.OutputStream;
import java.util.Date;
import java.util.SortedSet;

/**
 * Reading fileSets.
 *
 */
public interface VersionedFileSetRead {
    public static final int NO_FILESET = 0;

    int getEffective(String name);

    Date getInstallTime(String name, int version);

    SortedSet<Integer> getVersions(String name);

    SortedSet<String> getRelativePaths(String name, int version);

    void downloadItem(String name, int version, String item, OutputStream target);

    void downloadFileSetAsZip(String name, int version, OutputStream target);
}
