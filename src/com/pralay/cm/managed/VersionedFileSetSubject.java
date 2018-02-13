package com.pralay.cm.managed;

public interface VersionedFileSetSubject {

    /**
     * Attaches a config observer
     * observer's {@link VersionedFileSetObserver#update(VersionedFileSet, String, int)} update(com.pralay.cm.managed.apply.ConfigDiff)} is called during {@link #apply(String, int, int)}
     *
     * @param observer
     */
    void attach(VersionedFileSetObserver observer);

    /**
     * Detaches the config observer
     *
     * @param observer
     */
    void detach(VersionedFileSetObserver observer);
}
