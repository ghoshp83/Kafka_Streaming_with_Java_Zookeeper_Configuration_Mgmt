package com.pralay.cm;

import com.google.common.collect.Lists;
import org.apache.commons.configuration.AbstractConfiguration;

import java.util.List;

public class AdditionalConfigurationRegistry {
    private final List<AbstractConfiguration> additionalConfigurations = Lists.newArrayList();

    public void add(final AbstractConfiguration configuration) {
        additionalConfigurations.add(configuration);
    }

    public List<AbstractConfiguration> getAdditionalConfigurations() {
        return additionalConfigurations;
    }
}
