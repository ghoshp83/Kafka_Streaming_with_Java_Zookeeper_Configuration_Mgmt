package com.pralay.cm.kafka;

import com.pralay.cm.CMContext;
import com.google.inject.Injector;

/**
 * Injector for platform supported APIs
 * * {@link com.pralay.cm.kafka.KafkaClientFactory}
 */
abstract public class KafkaCMContext {

    protected static Injector localInjector() {
        return localInjector(CMContext.defaultConfigName());
    }

    protected static Injector localInjector(String defaultConfigName, String... additionalConfigs) {
        return CMContext
                .injectorBuilder()
                .withAdditionalModules(new KafkaCMModule())
                .withDefaultConfiguration(defaultConfigName)
                .withResourceURLs(additionalConfigs)
                .build();
    }

    public static CMContext.InjectorBuilder injectorBuilder() {
        CMContext.InjectorBuilder builder = CMContext.injectorBuilder();
        builder.withAdditionalModules(new KafkaCMModule(), new KafkaCMManagingModule());
        return builder;
    }
}
