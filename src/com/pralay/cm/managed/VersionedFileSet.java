package com.pralay.cm.managed;

import com.pralay.cm.VersionedFileSetRead;

import java.io.InputStream;

/**
 * Managing filesets related to an appsuite.
 *
 */
public interface VersionedFileSet extends VersionedFileSetRead {
    public static final int FIRST_FILESET_ID = NO_FILESET + 1;

    /**
     *  Installs a fileSet
     *
     * @param name
     * @param effectiveVersion
     * @param fileSetAsZip
     * @return new version
     */
    int install(String name, int effectiveVersion, InputStream fileSetAsZip);

    /**
     * Delete fileset.
     *
     * @param name
     * @param version
     */
    void delete(String name, int version);

    Lock lock(String name, boolean write);

    public static interface Lock extends AutoCloseable {
        public void close();
    }
}
