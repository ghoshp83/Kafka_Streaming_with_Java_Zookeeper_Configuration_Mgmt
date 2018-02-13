package com.pralay.cm.managed;

import com.pralay.cm.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Config store in ZK storing externals under:
 * /platform/appsuites/<appsuite>/config/<configId>
 * <p/>
 */
@Singleton
public class FileSetConfigStore extends AppSuiteVersionedFileSet implements
        ConfigStore,
        ConfigSubject,
        VersionedFileSetObserver // observer of appsuite changes
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSetConfigStore.class);

    private static final Set<String> PARSED_CONFIG_NAMES = ImmutableSet.of(CMContext.defaultConfigName());

    public static final String CONFIG_FILESET = "config";

    private final Decoder decoder;
    private final MetaStore metaStore;

    private final AtomicInteger metaVersion = new AtomicInteger(VersionedFileSetRead.NO_FILESET);

    private final Set<ConfigObserver> configObservers = Sets.newConcurrentHashSet();

    @Inject
    public FileSetConfigStore(final VersionedFileSet versionedFileSet, final MetaStore metaStore, final Decoder decoder) {
        super(versionedFileSet);
        this.metaStore = metaStore;
        this.decoder = decoder;
        LOGGER.debug("initializing {}", FileSetConfigStore.class.getName());
    }


    @Override
    public DefaultConfiguration getConfiguration(String appsuite, int version) {
        if (version == NO_FILESET)
            return new DefaultConfiguration(decoder);
        try (VersionedFileSet.Lock appsuiteLock = lock(appsuite, false)) {
            return getConfigNoLock(appsuite, version);
        } catch (Exception e) {
            throw new AppsuiteConfigurationException(appsuite, "cannot retrieve config ids list. " + e.getMessage(), e);
        }
    }

    @Override
    public void attach(final ConfigObserver observer) {
        configObservers.add(observer);
    }

    @Override
    public void detach(final ConfigObserver observer) {
        configObservers.remove(observer);
    }

    @Override
    public void update(VersionedFileSet versionedFileSet, String appsuite, int newMetaVersion) {
        if (versionedFileSet == metaStore) {
            validate(appsuite, newMetaVersion, getEffective(appsuite));
            metaVersion.set(newMetaVersion);
        }
    }

    @Override
    protected void postInstall(String appsuite, int effective, int candidateAppsuiteFileSetVersion) {
        validate(appsuite, metaStore.getEffective(appsuite), candidateAppsuiteFileSetVersion);
        for (ConfigObserver observer : configObservers)
            try {
                observer.update(
                        appsuite,
                        getConfigNoLock(appsuite, effective),
                        getConfigNoLock(appsuite, candidateAppsuiteFileSetVersion)
                );
            } catch (ConfigurationException e) {
                throw new AppsuiteConfigurationException(appsuite, e.getMessage(), e.getCause());
            } catch (Exception e) {
                throw new AppsuiteConfigurationException(appsuite, "cannot apply configuration due to exception. " + e.getMessage(), e);
            }
    }

    @Override
    protected String getAppsuitePath(String appsuite) {
        if (StringUtils.isBlank(appsuite))
            throw new ConfigurationException("appsuite name is blank");

        return appsuite + "/" + CONFIG_FILESET;
    }

    protected void validate(final String appsuite, final int metaVersion, final int configVersion) {
        try {
            if (metaVersion > VersionedFileSet.NO_FILESET && configVersion > NO_FILESET) {
                final Configuration config = getConfigNoLock(appsuite, configVersion);
                final String initialExternalRelative = "init/" + CMContext.defaultConfigName();
                if (metaStore.getRelativePaths(appsuite, metaVersion).contains(initialExternalRelative)) {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    metaStore.downloadItem(appsuite, metaVersion, initialExternalRelative, baos);
                    PropertiesTemplateValidator validator = new PropertiesTemplateValidator(
                            new ByteArrayInputStream(baos.toByteArray())
                    );
                    Map<String, String> validationResult = validator.validate(config);
                    if (!validationResult.isEmpty()) {
                        throw new AppsuiteFileSetValidationException(appsuite, CMContext.defaultConfigName(), validationResult);
                    }
                } else {
                    LOGGER.debug("nothing to validate - {} is missing from appsuite", initialExternalRelative);
                }
            }
        } catch (ConfigurationException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    private DefaultConfiguration getConfigNoLock(String appsuite, int version) throws Exception {
        DefaultConfiguration ret = new DefaultConfiguration(decoder);
        if (version != NO_FILESET) {
            ret.addConfiguration(getConfiguration(this, appsuite, version, null));
            if (metaVersion.get() == VersionedFileSetRead.NO_FILESET) {
                metaVersion.compareAndSet(VersionedFileSetRead.NO_FILESET, metaStore.getEffective(appsuite));
            }
            if (metaVersion.get() != VersionedFileSetRead.NO_FILESET)
                ret.addConfiguration(getConfiguration(metaStore, appsuite, metaVersion.get(), "resources"));
        }
        return ret;
    }

    private AbstractConfiguration getConfiguration(VersionedFileSetRead source, String appsuite, int version, String prefix) throws IOException {
        SortedSet<String> subPaths = source.getRelativePaths(appsuite, version);
        ImmutableMap.Builder<String, String> mapsBuilder = ImmutableMap.builder();
        for (String subPath : subPaths) {
            if (isManagedConfigItem(subPath) && (StringUtils.isBlank(prefix) || subPath.startsWith(prefix + "/"))) {
                Properties p = new Properties();
                LOGGER.info("reading configuration from {}", subPath);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                source.downloadItem(appsuite, version, subPath, baos);
                p.load(new ByteArrayInputStream(baos.toByteArray()));
                mapsBuilder.putAll(Maps.fromProperties(p));
            }
        }
        return new MapConfiguration(mapsBuilder.build());
    }

    private boolean isManagedConfigItem(String subPath) {
        return CMContext.defaultConfigName().equals(Paths.get(subPath).getFileName().toString());
    }

    private String getAppsuiteConfigPath(final String appsuite) {
        return appsuite + "/" + CONFIG_FILESET;
    }
}
