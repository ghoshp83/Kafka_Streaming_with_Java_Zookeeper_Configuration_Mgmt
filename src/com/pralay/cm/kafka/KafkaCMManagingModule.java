package com.pralay.cm.kafka;

import com.pralay.cm.managed.apply.ConfigApply;
import com.google.inject.AbstractModule;

public class KafkaCMManagingModule extends AbstractModule {
    @Override
    public void configure() {
        binder().bind(KafkaAdmin.class).to(KafkaAdminFromZK.class).asEagerSingleton();
        binder().bind(KafkaConfigObserver.class).asEagerSingleton();
        binder().bind(ConfigApply.class).to(KafkaConfigApply.class).asEagerSingleton();
    }
}
