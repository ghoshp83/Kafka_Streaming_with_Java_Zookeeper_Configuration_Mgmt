package com.pralay.cm;

public class AppsuiteConfigurationException extends ConfigurationException {
    private final String appsuite;

    public AppsuiteConfigurationException(String appsuite, String message) {
        super(message);
        this.appsuite = appsuite;
    }

    public AppsuiteConfigurationException(String appsuite, String message, Throwable cause) {
        super(message, cause);
        this.appsuite = appsuite;
    }

    public AppsuiteConfigurationException(String appsuite, Throwable cause) {
        super(cause);
        this.appsuite = appsuite;
    }

    public String getAppSuite() {
        return appsuite;
    }
}
