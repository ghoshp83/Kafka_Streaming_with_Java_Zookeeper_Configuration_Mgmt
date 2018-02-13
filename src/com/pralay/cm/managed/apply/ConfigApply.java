package com.pralay.cm.managed.apply;

import org.apache.commons.configuration.Configuration;

/**
 * Config management API for applications which intend to change configuration
 */
public interface ConfigApply {
    /**
     *
     * Called by applications in order to apply configuration
     *
     * @param appsuite apply config to appsuite
     * @param candidate the new candidate configuration
     * @throws com.pralay.cm.ConfigurationException
     */
    void apply(String appsuite, Configuration candidate);

    /**
     * Get applied configuration
     *
     * @param appsuite
     * @return
     */
    Configuration getApplied(String appsuite);
}
