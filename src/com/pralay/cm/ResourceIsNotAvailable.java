package com.pralay.cm;

public class ResourceIsNotAvailable extends ConfigurationException {
    public ResourceIsNotAvailable(String message) {
        super(message);
    }

    public ResourceIsNotAvailable(String message, Throwable cause) {
        super(message, cause);
    }

    public ResourceIsNotAvailable(Throwable cause) {
        super(cause);
    }
}
