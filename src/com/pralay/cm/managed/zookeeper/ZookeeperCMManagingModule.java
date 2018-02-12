package com.pralay.cm.managed.zookeeper;

import com.pralay.cm.Decoder;
import com.pralay.cm.MetaStore;
import com.pralay.cm.managed.VersionedFileSet;
import com.pralay.cm.managed.*;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;

import javax.inject.Singleton;
import java.util.concurrent.atomic.AtomicReference;

public class ZookeeperCMManagingModule extends AbstractModule {
    @Override
    public void configure() {
        binder().bind(ConfigStore.class).to(FileSetConfigStore.class).asEagerSingleton();
        binder().bind(MetaStore.class).to(AppSuiteMetaStore.class).asEagerSingleton();
        binder().bind(ConfigSubject.class).to(FileSetConfigStore.class).asEagerSingleton();
        binder().bind(AppSuiteResources.class).to(ZookeeperAppSuiteResources.class).asEagerSingleton();
    }


    private AtomicReference<FileSetConfigStore> fileSetConfigStore = new AtomicReference<>();

    @Provides
    @Singleton
    public FileSetConfigStore getConfigStore(final VersionedFileSet fileSet, final AppSuiteMetaStore metaStore, final Decoder decoder) {
        if (fileSetConfigStore.get() == null) {
            FileSetConfigStore update = new FileSetConfigStore(fileSet, metaStore, decoder);
            if (fileSetConfigStore.compareAndSet(null, update))
                metaStore.attach(update);
        }
        return fileSetConfigStore.get();
    }
}
