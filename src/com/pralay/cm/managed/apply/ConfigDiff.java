package com.pralay.cm.managed.apply;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;

import java.util.Set;

public class ConfigDiff {
    private final Set<String> changedPrefixes;

    private final Configuration candidate;

    private final Configuration effective;

    private final MapDifference diff;

    private final String appsuite;

    private ConfigDiff(final String appsuite, final Configuration candidate, final Configuration effective) {
        MapDifference diff = Maps.difference(ConfigPropertyUtils.toMap(candidate), ConfigPropertyUtils.toMap(effective));

        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        builder.addAll(ConfigPropertyUtils.getKeysWithParents(diff.entriesOnlyOnLeft().keySet()));
        builder.addAll(ConfigPropertyUtils.getKeysWithParents(diff.entriesDiffering().keySet()));
        builder.addAll(ConfigPropertyUtils.getKeysWithParents(diff.entriesOnlyOnRight().keySet()));

        this.appsuite = appsuite;
        this.changedPrefixes = builder.build();
        this.diff = diff;
        this.effective = effective;
        this.candidate = candidate;
    }

    public static ConfigDiff diff(final String appsuite, final Configuration candidate, final Configuration effective) {
        return new ConfigDiff(appsuite, candidate, effective);
    }

    static ConfigDiff diff(final Configuration candidate, final Configuration effective) {
        return diff(null, candidate, effective);
    }

    public String getAppSuite() {
        return appsuite;
    }

    public boolean isChanged(final String prefix) {
        return changedPrefixes.contains(prefixTrimmed(prefix));
    }

    /**
     * Returns purely added children
     *
     * @param prefix prefix keys
     * @return all the direct children keys of the prefix whose all child keys are added and there were no existing child keys before
     */
    public Set<String> getAddedChildren(final String prefix) {
        String prefixTrimmed = prefixTrimmed(prefix);
        Set<String> addedChildren = Sets.newHashSet(ConfigPropertyUtils.getDirectChildren(diff.entriesOnlyOnLeft().keySet(), prefixTrimmed));
        addedChildren.removeAll(ConfigPropertyUtils.getDirectChildren(ImmutableSet.copyOf(effective.getKeys()), prefixTrimmed));
        return addedChildren;
    }

    /**
     * Returns updated children
     *
     * @param prefix parent key
     * @return all the direct children keys of parent which had any existing child keys before, and those children are changed (added, updated or deleted)
     */
    public Set<String> getUpdatedChildren(final String prefix) {
        String prefixTrimmed = prefixTrimmed(prefix);
        Set<String> updatedChildren = Sets.newHashSet(ConfigPropertyUtils.getDirectChildren(Sets.newHashSet(effective.getKeys()), prefixTrimmed));
        updatedChildren.removeAll(getAddedChildren(prefixTrimmed));
        updatedChildren.removeAll(getRemovedChildren(prefixTrimmed));
        updatedChildren.retainAll(this.changedPrefixes);
        return updatedChildren;
    }


    /**
     * Returns fully removed children
     *
     * @param parent parent keys
     * @return all the direct children keys whose all child keys are removed
     */
    public Set<String> getRemovedChildren(String parent) {
        Set<String> removedChildren = Sets.newHashSet(ConfigPropertyUtils.getDirectChildren(Sets.newHashSet(effective.getKeys()), parent));
        removedChildren.removeAll(Sets.newHashSet(ConfigPropertyUtils.getDirectChildren(Sets.newHashSet(candidate.getKeys()), parent)));
        return removedChildren;
    }


    public Configuration getCandidate() {
        return candidate;
    }

    public Configuration getEffective() {
        return effective;
    }

    public MapDifference getDiff() {
        return diff;
    }

    private String prefixTrimmed(String prefix) {
        return prefix.endsWith(".") ? prefix.substring(0, prefix.length()-1) : prefix;
    }

    @Override
    public String toString() {
        return "ConfigDiff " + diff;
    }
}