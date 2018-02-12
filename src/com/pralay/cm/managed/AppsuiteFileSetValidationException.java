package com.pralay.cm.managed;

import com.pralay.cm.AppsuiteConfigurationException;

import java.util.Map;

public class AppsuiteFileSetValidationException extends AppsuiteConfigurationException {
    private final String name;
    private final Map<String, String> validationResult;

    public AppsuiteFileSetValidationException(final String appsuite, final String name, final Map<String, String> validationResult) {
        super(appsuite, "Invalid configuration[" + name +"]");
        this.name = name;
        this.validationResult = validationResult;
    }

    public String getName() {
        return name;
    }


    public Map<String, String> getValidationResult() {
        return validationResult;
    }
}
