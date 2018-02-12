package com.pralay.cm;

import com.pralay.cm.managed.ConfigInstallFromResources;
import com.pralay.cm.managed.DefaultConfigInstallFromResources;
import com.pralay.cm.managed.FileSetConfigStore;
import com.pralay.cm.managed.VersionedFileSet;
import com.pralay.cm.managed.zookeeper.ZookeeperVersionedFileSet;
import com.google.inject.Provides;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;

import javax.inject.Inject;
import javax.inject.Singleton;

public class CMModule extends CMBaseModule {
    private final CMContext.InjectorBuilder builder;

    public CMModule() {
        this.builder = CMContext.injectorBuilder();
    }

    public CMModule(final CMContext.InjectorBuilder builder) {
        this.builder = builder;
    }

    @Override
    public void configure() {
        super.configure();
        binder().bind(AppsuiteContext.class).toInstance(new AppsuiteContext() {
            @Override
            public String getAppsuite() {
                return CMModule.this.getAppsuite();
            }
        });
        if (!StringUtils.isBlank(getAppsuite()))
            binder().bind(Decoder.class).to(AppsuiteResourceDecoder.class).asEagerSingleton();
        else
            binder().bind(Decoder.class).to(DefaultDecoder.class).asEagerSingleton();
        binder().bind(ConfigStoreRead.class).to(FileSetConfigStore.class).asEagerSingleton();
        binder().bind(ConfigInstallFromResources.class).to(DefaultConfigInstallFromResources.class).asEagerSingleton();
    }

    protected String getAppsuite() {
        return builder != null && builder.getInitBuilder().getAppsuite() != null
                ?
                builder.getInitBuilder().getAppsuite()
                :
                AppConfigManager.getAppsuite();
    }

    @Provides
    @Singleton
    public VersionedFileSet getVersionedFileSet(final CuratorFramework curatorFramework) {
        return new ZookeeperVersionedFileSet("/platform/appsuites", curatorFramework);
    }


    @Provides
    @Singleton
    @Inject
    public APPConfiguration getConfiguration(ConfigStoreRead configStoreRead) {
        if (!StringUtils.isBlank(getAppsuite()))
            builder
                    .withConfigurations(
                            configStoreRead.getConfiguration(
                                    getAppsuite(),
                                    configStoreRead.getEffective(builder.getInitBuilder().getAppsuite())
                            )
                    );

        return builder.getInitBuilder().init();
    }
}
