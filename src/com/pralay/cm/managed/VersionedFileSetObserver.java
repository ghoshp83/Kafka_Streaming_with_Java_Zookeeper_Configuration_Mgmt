package com.pralay.cm.managed;

public interface VersionedFileSetObserver {
    void update(VersionedFileSet versionedFileSet, String name, int version);
}
