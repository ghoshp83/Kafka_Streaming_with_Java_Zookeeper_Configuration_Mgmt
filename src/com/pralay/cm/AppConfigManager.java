package com.pralay.cm;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.config.ClasspathPropertiesConfiguration;
import com.netflix.config.ConfigurationManager;
import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Use {@link CMContext} instead
 * Frontend API for configuration management.
 *
 * @return
 */
@Deprecated
public abstract class AppConfigManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppConfigManager.class);

    public static final String ADDITIONAL_CONFIGS = "com.pralay.cm.additionalConfigs";
    public static final String APPSUITE_NAME = "appsuite.name";
    public static final String DEFAULT_CLASSPATH_CONFIG = "com.pralay.cm.defaultClasspathConfig";

    private static final ReadWriteLock initOrDestroy = new ReentrantReadWriteLock(true);

    private static volatile AtomicReference<com.pralay.cm.APPConfiguration> configuration = new AtomicReference<>();

    protected AppConfigManager() {}

    public static InitBuilder initBuilder() {
        return new InitBuilder() {
            @Override
            public com.pralay.cm.APPConfiguration init() {
                return AppConfigManager.initialize(super.init());
            }
        };
    }

    public static String getAppsuite() {
        return System.getProperty(APPSUITE_NAME);
    }

    static void setAppsuite(String appsuite) {
        System.setProperty(APPSUITE_NAME, appsuite);
    }

    /**
     * Use {@link CMContext#defaultConfigName()}
     *
     * @return
     */
    @Deprecated
    public static String defaultConfigName() {
        return CMContext.defaultConfigName();
    }

    /**
     *
     * Use {@link CMContext#localConfig() } instead
     *
     * @return singleton instance
     */
    public static APPConfiguration current() {
        return instance();
    }

    /**
     * Use {@link CMContext#localConfig() } instead
     *
     * @return
     */
    public static com.pralay.cm.APPConfiguration instance() {
        com.pralay.cm.APPConfiguration result = configuration.get();
        return result != null ? result : initialize(new InitBuilder().init());
    }

    private static com.pralay.cm.APPConfiguration initialize(final com.pralay.cm.APPConfiguration newConfig) {
        com.pralay.cm.APPConfiguration result = configuration.get();
        if (result == null) {
            Lock update = initOrDestroy.writeLock();
            update.lock();
            try {
                result = configuration.get();
                if (result == null) {
                    result = newConfig;
                    configuration.set(result);
                    LOGGER.info("configuration initialized");
                }
            } finally {
                update.unlock();
            }
        } else
            LOGGER.error("configuration  - already initialized");
        return result;
    }



    public static class InitBuilder {
        private List<String> urlResourcesToLoad = Lists.newArrayList();
        private String defaultClasspathConfiguration;
        private Decoder decoder = null;
        private String appsuite = AppConfigManager.getAppsuite();

        private AbstractConfiguration[] configurations = null;

        InitBuilder() {
            defaultClasspathConfiguration = defaultConfigName();
        }

        public InitBuilder skipClasspathConfig() {
            defaultClasspathConfiguration = null;
            return this;
        }

        public InitBuilder withResourceURLs(final String... resources) {
            urlResourcesToLoad.addAll(ImmutableList.copyOf(resources));
            return this;
        }

        public InitBuilder withAppsuite(final String appsuite) {
            this.appsuite = appsuite;
            return this;
        }

        public InitBuilder withDefaultConfiguration(final String defaultConfiguration) {
            this.defaultClasspathConfiguration = defaultConfiguration;
            return this;
        }

        public InitBuilder withDecoder(Decoder decoder) {
            this.decoder = decoder;
            return this;
        }

        public InitBuilder withConfigurations(AbstractConfiguration... configurations) {
            this.configurations = configurations;
            return this;
        }

        /**
         * Use {@link CMContext#injectorBuilder()} instead
         *
         * @return
         */
        public APPConfiguration init() {
            return buildConfig();
        }

        protected APPConfiguration buildConfig() {
            final DefaultConfiguration builtConfiguration = new DefaultConfiguration(decoder != null ? decoder : new DefaultDecoder());

            setupAdditionalConfiguration(builtConfiguration);

            if (configurations != null)
                for (AbstractConfiguration c : configurations)
                    builtConfiguration.addConfiguration(c);

            if (initClasspathConfiguration()) {
                builtConfiguration.addConfiguration(ConfigurationManager.getConfigInstance());
            }

            return builtConfiguration;
        }

        public String getAppsuite() {
            return appsuite;
        }

        private void setupAdditionalConfiguration(DefaultConfiguration builtConfiguration) {
            String additionalURLConfigs = System.getProperty(ADDITIONAL_CONFIGS);
            if (additionalURLConfigs != null)
                urlResourcesToLoad.addAll(ImmutableList.copyOf(Splitter.on(",").omitEmptyStrings().trimResults().split(additionalURLConfigs)));
            if (!urlResourcesToLoad.isEmpty()) {
                for (String resource : urlResourcesToLoad) {
                    try {
                        builtConfiguration.addConfiguration(APPConfigurations.getConfigFromResource(resource));
                        LOGGER.info("additional configuration {} added", resource);
                    } catch (Exception e) {
                        LOGGER.error("cannot add configuration " + resource, e);
                    }
                }
            }
        }

        private boolean initClasspathConfiguration() {
            if (defaultClasspathConfiguration != null) {
                APPConfigurations.ParsedURL url = APPConfigurations.parseResource(defaultClasspathConfiguration);
                if (url.isClassPathResource()) {
                    ClasspathPropertiesConfiguration.setPropertiesResourceRelativePath(defaultClasspathConfiguration);
                    try {
                        url.getUrl().openStream().close();
                        ClasspathPropertiesConfiguration.initialize();
                        LOGGER.info("default classpath configuration is initialized: {}", defaultClasspathConfiguration);
                        return true;
                    } catch (IOException e) {
                        LOGGER.info("cannot open default configuration: {}", defaultClasspathConfiguration);
                    }
                } else
                    LOGGER.info("only classpath configuration is accepted as default classpath configuration: {}", defaultClasspathConfiguration);
            } else
                LOGGER.info("no default classpath configuration is provided");
            return false;
        }
    }
}
