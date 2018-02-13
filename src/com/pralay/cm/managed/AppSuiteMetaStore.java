package com.pralay.cm.managed;

import com.pralay.cm.ConfigurationException;
import com.pralay.cm.MetaStore;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Set;

/**
 *
 * Directory structure:
 * ...
 *
 * External initial configuration is used for both documentation and validation:
 *
 *      # my property documentation
 *      /keySuffixRegexp/=/valueRegexp/
 *
 * Sample:
 *
 *      # my property documentation
 *      key./[a-z]+/=/[0-9]+/
 *
 * In the sample, the following entries are accepted:
 *
 *      key.apple=123
 *      key.pear=0
 *
 * But these are not:
 *
 *      key.X=123
 *      key.x=apple
 *
 */
@Singleton
public class AppSuiteMetaStore extends AppSuiteVersionedFileSet implements MetaStore, VersionedFileSetSubject {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppSuiteMetaStore.class);

    public static final String APPSUITE_META_FILESET = "meta";

    private final Set<VersionedFileSetObserver> versionedFileSetObservers = Sets.newConcurrentHashSet();

    @Inject
    public AppSuiteMetaStore(final VersionedFileSet versionedFileSet) {
        super(versionedFileSet);
        LOGGER.debug("initializing {}", AppSuiteMetaStore.class.getName());
    }

    @Override
    public void attach(VersionedFileSetObserver observer) {
        versionedFileSetObservers.add(observer);
    }

    @Override
    public void detach(VersionedFileSetObserver observer) {
        versionedFileSetObservers.remove(observer);
    }

    @Override
    protected void postInstall(String appsuite, int effective, int candidateAppsuiteFileSetVersion) {
        for (VersionedFileSetObserver observer : versionedFileSetObservers)
            observer.update(
                    this,
                    appsuite,
                    candidateAppsuiteFileSetVersion);
    }

    @Override
    protected String getAppsuitePath(String appsuite) {
        if (StringUtils.isBlank(appsuite))
            throw new ConfigurationException("appsuite name is blank");
        return appsuite + "/" + APPSUITE_META_FILESET;
    }
}
