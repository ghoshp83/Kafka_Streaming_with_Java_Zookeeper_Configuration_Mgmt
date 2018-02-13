package com.pralay.cm.managed;

import com.pralay.cm.AppsuiteContext;
import com.pralay.cm.ConfigStoreRead;
import com.pralay.cm.AppConfigManager;
import com.pralay.cm.APPConfigurations;
import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DefaultConfigInstallFromResources implements ConfigInstallFromResources {
    private final static Logger LOGGER = LoggerFactory.getLogger(DefaultConfigInstallFromResources.class);

    private final ConfigStore configStore;

    private final String appsuite;

    @Inject
    public DefaultConfigInstallFromResources(AppsuiteContext appsuiteContext, ConfigStore configStore) {
        this.appsuite = appsuiteContext.getAppsuite();
        this.configStore = configStore;
    }

    @Override
    public int installFromResources(String... resources) {
        int effective = configStore.getEffective(appsuite);

        try {
            Path tempDir = effective != ConfigStoreRead.NO_FILESET ? ConfigStoreUtils.downloadToTemp(configStore, appsuite, effective) : Files.createTempDirectory(DefaultConfigInstallFromResources.class.getSimpleName());
            boolean changed = false;
            for (String resource : resources) {
                String resourceName = new File(resource).getName();
                File tempResource = Paths.get(tempDir.toString(), resourceName).toFile();
                AbstractConfiguration configFromResource = APPConfigurations.getConfigFromResource(resource);
                if (!tempResource.exists() ||
                        !APPConfigurations.diff(
                                configFromResource,
                                APPConfigurations.getConfigFromURL(tempResource.toURI().toURL())
                        ).areEqual()) {
                    LOGGER.info("config {} changed", resourceName);
                    changed = true;
                    tempResource.delete();
                    try (OutputStream os = new FileOutputStream(tempResource)) {
                        APPConfigurations
                                .getProperties(configFromResource)
                                .store(new OutputStreamWriter(os), "");
                    }
                    return configStore.install(appsuite, effective, ConfigStoreUtils.zipDirToStream(tempDir.toString()));
                } else {
                    LOGGER.info("config {} is the same as the resource", resourceName);
                }
            }
            if (changed) {
                LOGGER.info("install changed");
                configStore.install(appsuite, effective, ConfigStoreUtils.zipDirToStream(tempDir.toString()));
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return effective;
    }
}
