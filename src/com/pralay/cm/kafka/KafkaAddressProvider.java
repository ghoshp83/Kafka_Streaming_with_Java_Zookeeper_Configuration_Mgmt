package com.pralay.cm.kafka;

import com.pralay.cm.ServiceAddressProvider;

import java.util.List;

public class KafkaAddressProvider extends ServiceAddressProvider {
    private final KafkaInfo kafkaInfo;

    public KafkaAddressProvider(final KafkaInfo kafkaInfo) {
        this.kafkaInfo = kafkaInfo;
    }

    @Override
    public List<String> getAddresses() {
        return kafkaInfo.getBrokerAddresses();
    }
}
