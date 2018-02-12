package com.pralay.cm;

/**
 * Config store management
 *
 */
public interface ConfigStoreRead extends VersionedFileSetRead {
    /**
     * Returns configuration content
     *
     * @param appsuite app suite
     * @param appsuite configuration version
     * @return
     */
    DefaultConfiguration getConfiguration(String appsuite, int version);
}
