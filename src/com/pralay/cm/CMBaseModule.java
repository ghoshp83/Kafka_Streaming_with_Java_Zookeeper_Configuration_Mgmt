package com.pralay.cm;

import com.pralay.core.configuration.ExecutionConfiguration;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.commons.configuration.Configuration;
import org.apache.curator.framework.CuratorFramework;

import javax.inject.Inject;
import javax.inject.Named;

public class CMBaseModule extends AbstractModule {

    @Override
    public void configure() {
        binder().bind(ExecutionConfiguration.class).toInstance(new ExecutionConfiguration());
        binder().bind(Configuration.class).to(APPConfiguration.class).asEagerSingleton();
        binder().bind(CuratorFramework.class).toProvider(CuratorProvider.class);
    }

    @Provides
    @Named(ServiceAddressProvider.ZOOKEEPER_SERVICE_QUALIFIER)
    @Inject
    public ServiceAddressProvider getZookeeperAddressProvider(final ExecutionConfiguration exeConfiguration) {
        return new ZookeeperAddressProvider(exeConfiguration);
    }
}
