package com.pralay.cm;

import com.netflix.config.WatchedUpdateListener;
import org.apache.commons.configuration.Configuration;

import java.util.Properties;

/**
 * Extending commons configuration API with
 * - type value resolving
 * - configuration watcher
 *
 */
public interface APPConfiguration extends Configuration {
    /**
     * Retrieves configuration as java Properties
     *
     * @return java properties
     */
    Properties getProperties();

    /**
     *
     * Initializes return type
     * Throws #{link ConfigurationException}
     *
     * @param type return type
     * @param path key or prefix
     * @param <T>
     *          In case path is a key:
     *           - #{link InputStream} is configured with value resource,
     *           - other types are populated from json (config value refers to a json resourceURL)
     *         In case path is a prefix, subconfiguration is populated to the bean.
     * @return
     */
    <T> T get(final Class<T> type, final String path);

    /**
     * Same as above but in case of lacking key, defaultValue is returned
     */
    <T> T get(final Class<T> type, final String path, T defaultValue);

    /**
     * Observing configuration change
     *
     * @param path path to be observed
     * @param observer observer to be called back
     */
    void observe(String path, WatchedUpdateListener observer);

    /**
     * Removing observer
     *
     * @param observer observer to be removed
     */
    void remove(WatchedUpdateListener observer);
}
