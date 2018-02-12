package com.pralay.cm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CMLocalModule extends CMBaseModule {
    private static final Logger LOGGER = LoggerFactory.getLogger(CMLocalModule.class);

    private final CMContext.InjectorBuilder builder;

    public CMLocalModule(final CMContext.InjectorBuilder builder) {
        this.builder = builder;
    }

    @Override
    public void configure() {
        super.configure();
        binder().bind(Decoder.class).to(DefaultDecoder.class).asEagerSingleton();
        binder().bind(APPConfiguration.class).toInstance(builder.getInitBuilder().init());
    }
}
