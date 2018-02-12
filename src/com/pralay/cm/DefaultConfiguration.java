package com.pralay.cm;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.netflix.config.ConcurrentCompositeConfiguration;
import com.netflix.config.WatchedUpdateListener;
import com.netflix.config.WatchedUpdateResult;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.SubsetConfiguration;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Default configuration class instantiated by {@link com.pralay.cm.AppConfigManager}
 *
 */
public class DefaultConfiguration extends ConcurrentCompositeConfiguration implements WatchedUpdateListener, APPConfiguration {

    private final Decoder decoder;

    private final BiMap<String,WatchedUpdateListener> observers = HashBiMap.create();


    private static final Gson gson = new Gson();

    public DefaultConfiguration(final Decoder decoder) {
        this.decoder = decoder;
    }

    @Override
    public Properties getProperties() {
        return APPConfigurations.getProperties(this);
    }

    @Override
    public <T> T get(Class<T> type, String path, T defaultValue) {
        return containsKey(path) ? decoder.decode(type, getString(path)) : defaultValue;
    }

    @Override
    public <T> T get(Class<T> type, String path) {
        if (!containsKey(path) && !type.isPrimitive())
            return getFromSubTree(this, type, path);
        else
            return decoder.decode(type, getString(path));
    }

    public static <T> T getFromSubTree(final Configuration configuration, final Class<T> type, final String path) {
        return gson.fromJson(gson.toJsonTree(Maps.fromProperties(APPConfigurations.getProperties(configuration.subset(path)))), type);
    }

    @Override
    public APPConfiguration subset(String key) {
        return new WatchableSubConfiguration(key != null ? key : "");
    }

    @Override
    public void observe(String path, WatchedUpdateListener observer) {
        observers.put(path, observer);
    }

    @Override
    public void remove(final WatchedUpdateListener observer) {
        observers.remove(observer);
    }


    @Override
    public void updateConfiguration(WatchedUpdateResult result) {
        for (Map.Entry<String,WatchedUpdateListener> pathEntry: observers.entrySet()) {
            if (result.isIncremental()) {
                Map<String, Object> addedKeys = inPath(pathEntry.getKey(), result.getAdded());
                Map<String, Object> changedKeys = inPath(pathEntry.getKey(), result.getChanged());
                Map<String, Object> removedKeys = inPath(pathEntry.getKey(), result.getDeleted());

                if (!addedKeys.isEmpty() || !changedKeys.isEmpty() || !removedKeys.isEmpty())
                    pathEntry.getValue().updateConfiguration(
                            WatchedUpdateResult.createIncremental(addedKeys, changedKeys, removedKeys)
                    );
            } else {
                Map<String, Object> complete = inPath(pathEntry.getKey(), result.getComplete());

                if (!complete.isEmpty())
                    pathEntry.getValue().updateConfiguration(WatchedUpdateResult.createFull(complete));
            }
        }
    }

    private Map<String,Object> inPath(String path, Map<String,Object> values) {
        if (values == null || values.isEmpty())
            return Collections.emptyMap();
        ImmutableMap.Builder<String,Object> inPath = ImmutableMap.builder();
        for (Map.Entry<String,Object> entry: values.entrySet()) {
            if ("".equals(path)) {
                inPath.put(entry.getKey(), entry.getValue());
            } else if (entry.getKey().startsWith(path+".")) {
                inPath.put(entry.getKey().substring((path+".").length()), entry.getValue());
            }
        }
        return inPath.build();
    }


    private class WatchableSubConfiguration extends SubsetConfiguration implements APPConfiguration {
        WatchableSubConfiguration(String prefix) {
            super(DefaultConfiguration.this, prefix, ".");
        }

        @Override
        public <T> T get(Class<T> type, String path, T defaultValue) {
            return DefaultConfiguration.this.get(type, getParentKey(path), defaultValue);
        }

        @Override
        public Properties getProperties() {
            return APPConfigurations.getProperties(this);
        }

        @Override
        public <T> T get(Class<T> type, String path) {
            return DefaultConfiguration.this.get(type, getParentKey(path));
        }

        @Override
        public void observe(String path, WatchedUpdateListener observer) {
            observers.put(getParentKey(path), observer);
        }

        @Override
        public void remove(WatchedUpdateListener observer) {
            observers.remove(observer);
        }
    }
}
