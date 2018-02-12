package com.pralay.cm.managed;

import com.pralay.cm.AppsuiteConfigurationException;

public class NoSuchConfigurationException extends AppsuiteConfigurationException {
    public NoSuchConfigurationException(String appsuite, int configurationId) {
        super(appsuite, "no such configuration[" + configurationId +"]");
    }

    public NoSuchConfigurationException(String appsuite, int configurationId, Throwable cause) {
        super(appsuite, "no such configuration[" + configurationId +"]", cause);
    }
}
