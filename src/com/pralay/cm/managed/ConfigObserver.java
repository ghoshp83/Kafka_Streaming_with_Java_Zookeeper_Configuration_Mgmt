package com.pralay.cm.managed;

import org.apache.commons.configuration.Configuration;

public interface ConfigObserver {
    void update(String appsuite, Configuration effective, Configuration candidate);
}
