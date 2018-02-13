package com.pralay.cm;

import com.pralay.cm.managed.ConfigInstallFromResources;
import com.pralay.cm.managed.zookeeper.ZookeeperCMManagingModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.Configuration;

/**
 * Injector for platform supported APIs
 * * {@link org.apache.curator.framework.CuratorFramework}
 * * {@link ConfigInstallFromResources}
 * * {@link APPConfiguration}
 * * {@link Configuration}
 */
abstract public class CMContext {
    private static final String DEFAULT_CONFIGURATION_NAME = "application.properties";

    public static String defaultConfigName() {
        return System.getProperty(AppConfigManager.DEFAULT_CLASSPATH_CONFIG, DEFAULT_CONFIGURATION_NAME);
    }


    public static APPConfiguration currentConfig(final Injector injector) {
        return injector.getInstance(APPConfiguration.class);
    }

    public static InjectorBuilder injectorBuilder() {
        return new InjectorBuilder();
    }

    public static APPConfiguration localConfig() {
        return localConfig(defaultConfigName());
    }

    /**
     * Direct access to local configuration (config store is skipped)
     *
     * @param defaultConfigName
     * @param additionalConfigs
     * @return
     */
    public static APPConfiguration localConfig(final String defaultConfigName, final String... additionalConfigs) {
        return localInjector(defaultConfigName, additionalConfigs).getInstance(APPConfiguration.class);
    }

    protected static Injector localInjector(String defaultConfigName, String... additionalConfigs) {
        return injectorBuilder().withDefaultConfiguration(defaultConfigName).withResourceURLs(additionalConfigs).build();
    }

    public static class InjectorBuilder {
        private final AppConfigManager.InitBuilder initBuilder = new AppConfigManager.InitBuilder();
        private final ImmutableList.Builder<Module> modulesBuilder = ImmutableList.builder();
        private boolean managed = false;

        private InjectorBuilder() {
        }

        public InjectorBuilder skipClasspathConfig() {
            initBuilder.skipClasspathConfig();
            return this;
        }

        public InjectorBuilder setManaged() {
            this.managed = true;
            return this;
        }

        public InjectorBuilder withResourceURLs(final String... resources) {
            initBuilder.withResourceURLs(resources);
            return this;
        }

        public InjectorBuilder withAppsuite(final String appsuite) {
            initBuilder.withAppsuite(appsuite);
            return this;
        }

        public InjectorBuilder withDefaultConfiguration(final String defaultConfiguration) {
            initBuilder.withDefaultConfiguration(defaultConfiguration);
            return this;
        }

        public InjectorBuilder withDecoder(Decoder decoder) {
            initBuilder.withDecoder(decoder);
            return this;
        }

        public InjectorBuilder withConfigurations(AbstractConfiguration... configurations) {
            initBuilder.withConfigurations(configurations);
            return this;
        }

        public InjectorBuilder withAdditionalModules(Module... additionals) {
            if (additionals != null)
                modulesBuilder.add(additionals);
            return this;
        }

        AppConfigManager.InitBuilder getInitBuilder() {
            return initBuilder;
        }

        public Injector build() {
            if (managed)
                modulesBuilder.add(new CMModule(this), new ZookeeperCMManagingModule());
            else
                modulesBuilder.add(new CMLocalModule(this));
            return Guice.createInjector(modulesBuilder.build());
        }
    }
}
