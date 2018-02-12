package com.pralay.cm;

import java.util.List;

public abstract class ServiceAddressProvider {
    public static final String ZOOKEEPER_SERVICE_QUALIFIER = "zookeeper";
    public static final String KAFKA_SERVICE_QUALIFIER = "kafka";

    public abstract List<String> getAddresses();


    public static String getAddressKey(final String serviceQualifier) {
        return serviceQualifier + ".address";
    }
}
