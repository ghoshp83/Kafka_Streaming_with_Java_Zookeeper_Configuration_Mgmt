package com.pralay.cm.kafka;

import com.pralay.cm.ServiceAddressProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;

import javax.inject.Inject;
import javax.inject.Named;

public class KafkaCMModule extends AbstractModule {
    @Override
    public void configure() {
        binder().bind(KafkaInfo.class).to(KafkaInfoFromZK.class).asEagerSingleton();
        binder().bind(KafkaClientFactory.class).to(DefaultKafkaClientFactory.class).asEagerSingleton();
    }

    @Provides
    @Named(ServiceAddressProvider.KAFKA_SERVICE_QUALIFIER)
    @Inject
    public ServiceAddressProvider getKafkaAddressProvider(final KafkaInfo kafkaInfo) {
        return new KafkaAddressProvider(kafkaInfo);
    }
}
