package com.pralay.cm;

import com.pralay.cm.managed.ConfigStore;

import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;

public class AppsuiteResourceDecoder extends DefaultDecoder {
    private final String appsuite;

    private final MetaStore metaStore;
    private final AtomicInteger metaVersion = new AtomicInteger();

    private final ConfigStore configStore;
    private final AtomicInteger configVersion = new AtomicInteger();

    @Inject
    public AppsuiteResourceDecoder(AppsuiteContext appsuiteContext, MetaStore metaStore, ConfigStore configStore ) {
        this.appsuite = appsuiteContext.getAppsuite();
        this.metaStore = metaStore;
        this.configStore = configStore;
    }

    @Override
    public <T> T decode(Class<T> type, String encoded) {
        // lazy calculation
        if (metaVersion.get() == VersionedFileSetRead.NO_FILESET) {
            metaVersion.compareAndSet(VersionedFileSetRead.NO_FILESET, metaStore.getEffective(appsuite));
        }
        if (configVersion.get() == VersionedFileSetRead.NO_FILESET) {
            configVersion.compareAndSet(VersionedFileSetRead.NO_FILESET, configStore.getEffective(appsuite));
        }
        if (type.equals(InputStream.class)) {
            ByteArrayOutputStream baos = null;
            if (configStore.getRelativePaths(appsuite, configVersion.get()).contains(encoded)) {
                baos = new ByteArrayOutputStream();
                configStore.downloadItem(appsuite, configVersion.get(), encoded, baos);
            } else if (metaStore.getRelativePaths(appsuite, metaVersion.get()).contains("resources/" + encoded)) {
                baos = new ByteArrayOutputStream();
                metaStore.downloadItem(appsuite, metaVersion.get(), "resources/" + encoded, baos);
            }
            if (baos == null) {
                return super.decode(type, encoded);
            } else
                return type.cast(new ByteArrayInputStream(baos.toByteArray()));
        } else
            return super.decode(type, encoded);
    }
}
