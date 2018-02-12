package com.pralay.cm.kafka;

import com.pralay.cm.*;
import com.pralay.cm.managed.ConfigStore;
import com.pralay.cm.managed.ConfigStoreUtils;
import com.pralay.cm.managed.apply.ConfigApply;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class KafkaConfigApply implements ConfigApply {
    private final ConfigStore configStore;
    private final KafkaConfigFromService kafkaConfigFromService;

    @Inject
    public KafkaConfigApply(final ConfigStore configStore, final KafkaConfigFromService kafkaConfigFromService) {
        this.configStore = configStore;
        this.kafkaConfigFromService = kafkaConfigFromService;
    }

    @Override
    public void apply(String appsuite, Configuration candidate) {
        Path effectiveZip = null;
        Path targetZip = null;
        Path tempDir = null;
        try {
            tempDir = Files.createTempDirectory(KafkaConfigApply.class.getName());
            int effective = configStore.getEffective(appsuite);

            if (effective != VersionedFileSetRead.NO_FILESET) {
                // download currentConfig config to tempDir
                effectiveZip = Files.createTempFile(KafkaConfigApply.class.getName() + "-", ".zip");
                FileOutputStream target = new FileOutputStream(effectiveZip.toFile());
                configStore.downloadFileSetAsZip(appsuite, effective, target);
                ConfigStoreUtils.copyToDir(new FileInputStream(effectiveZip.toFile()), tempDir.toFile());
            }

            // replace application.properties with candidate
            new File(tempDir.toFile(), CMContext.defaultConfigName()).delete();
            Properties candidateProperties = APPConfigurations.getProperties(candidate);
            try (FileOutputStream fos = new FileOutputStream(new File(tempDir.toFile(), CMContext.defaultConfigName()))) {
                candidateProperties.store(fos, "");
            }

            // apply configuration
            targetZip = Files.createTempFile(KafkaConfigApply.class.getName() + "-", ".zip");
            ConfigStoreUtils.copyToZip(tempDir, targetZip);
            configStore.install(appsuite, effective, new FileInputStream(targetZip.toFile()));
        } catch (IOException e) {
            throw new ConfigurationException(e);
        } finally {
            for (Path toDelete: new Path[] {effectiveZip, targetZip, tempDir})
                if (toDelete != null)
                    FileUtils.deleteQuietly(toDelete.toFile());
        }
    }


    @Override
    public Configuration getApplied(String appsuite) {
        return kafkaConfigFromService.getConfigFromService(appsuite);
    }
}
