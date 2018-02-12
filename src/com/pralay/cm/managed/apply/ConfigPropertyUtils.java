package com.pralay.cm.managed.apply;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConfigPropertyUtils {
    public static final char SEPARATOR = '.';

    private static final Splitter SPLITTER = Splitter.on(SEPARATOR);
    private static final Joiner JOINER = Joiner.on(SEPARATOR);

    public static Map<String, Object> toMap(Configuration source) {
        ImmutableMap.Builder builder = ImmutableMap.<String, Object>builder();
        for (String key: Sets.newHashSet((Iterator<String>)source.getKeys())) {
            builder.put(key, source.getProperty(key));
        }
        return builder.build();
    }

    public static Set<String> getKeysWithParents(Iterable<String> keys) {
        return getChildren(keys, null, 0);
    }

    public static Set<String> getChildren(Iterable<String> keys, String prefix, int fromLevel) {
        return getKeysWithParents(keys, prefix, fromLevel, Integer.MAX_VALUE);
    }

    public static Set<String> getDirectChildren(Iterable<String> keys, String prefix) {
        return getKeysWithParents(keys, prefix, 1, 1);
    }

    public static Set<String> getKeysWithParents(Iterable<String> keys, String prefix, int childMinLevel, int childMaxLevel) {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (String key: keys) {
            List<String> split = SPLITTER.splitToList(key);
            for (int i = 1; i <= split.size(); i++) {
                String joined = JOINER.join(split.subList(0, i));
                int childLevel = i - (prefix == null ? 0 : SPLITTER.splitToList(prefix).size());
                String dottedEndPrefix = prefix == null ? prefix : (prefix.endsWith(".") ? prefix : prefix + ".");
                if ((prefix == null || joined.startsWith(dottedEndPrefix)) && childMinLevel <= childLevel && childLevel <= childMaxLevel) {
                    builder.add(joined);
                }
            }
        }
        return builder.build();
    }

    public static Set<String> stripPrefix(final Iterable<String> keys, final String prefix) {
        String p = prefix.endsWith(".") ? prefix : prefix + ".";
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (String key: keys) {
            if (key.startsWith(p))
                builder.add(key.substring(p.length()));
        }
        return builder.build();
    }


    public static Set<String> leaves(final Iterable<String> keys) {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (String key: keys) {
            List<String> split = SPLITTER.splitToList(key);
            builder.add(split.get(split.size()-1));
        }
        return builder.build();
    }


    public static Set<String> roots(final Iterable<String> keys) {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (String key: keys) {
            List<String> split = SPLITTER.splitToList(key);
            builder.add(split.get(0));
        }
        return builder.build();
    }


    public static Set<String> elementsAfterPrefix(Iterable<String> src, String prefix) {
        return ConfigPropertyUtils.roots(
                ConfigPropertyUtils.stripPrefix(
                        src, prefix
                )
        );
    }

}