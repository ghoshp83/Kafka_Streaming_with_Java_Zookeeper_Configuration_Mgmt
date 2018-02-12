package com.pralay.cm.managed;

import com.pralay.cm.AppsuiteConfigurationException;
import com.pralay.cm.ConfigurationException;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Set;
import java.util.SortedSet;

/**
 * Config store in ZK storing externals under:
 * /platform/appsuites/<appsuite>/config/<configId>
 * <p/>
 */
@Singleton
public abstract class AppSuiteVersionedFileSet implements VersionedFileSet
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AppSuiteVersionedFileSet.class);

    private final VersionedFileSet versionedFileSet;

    private final Set<ConfigObserver> configObservers = Sets.newConcurrentHashSet();

    @Inject
    public AppSuiteVersionedFileSet(final VersionedFileSet versionedFileSet) {
        this.versionedFileSet = versionedFileSet;
    }

    @Override
    public Date getInstallTime(String appsuite, int configurationId) {
        return versionedFileSet.getInstallTime(getAppsuitePath(appsuite), configurationId);
    }

    @Override
    public int getEffective(String appsuite) {
        try (Lock appsuiteLock = versionedFileSet.lock(appsuite, false)) {
            return versionedFileSet.getEffective(getAppsuitePath(appsuite));
        } catch (Exception e) {
            throw new AppsuiteConfigurationException(appsuite, "cannot retrieve applied configuration", e);
        }
    }

    @Override
    public int install(String appsuite, int effectiveVersion, InputStream resourcesAsZip) {
        boolean exceptionThrown = false;
        int candidateAppsuiteFileSetVersion = NO_FILESET;
        String appsuitePath = getAppsuitePath(appsuite);
        try (Lock appsuiteLock = versionedFileSet.lock(appsuite, true)) {
            int effective = versionedFileSet.getEffective(appsuitePath);
            if (effectiveVersion != effective) {
                throw new ConcurrentModificationException(appsuite, candidateAppsuiteFileSetVersion, effectiveVersion);
            }

            candidateAppsuiteFileSetVersion = versionedFileSet.install(appsuitePath, effectiveVersion, resourcesAsZip);;
            postInstall(appsuite, effective, candidateAppsuiteFileSetVersion);

            LOGGER.info("new appsuite[{}] fileSet installed[{}] to [{}]", appsuite, candidateAppsuiteFileSetVersion, appsuitePath);
            return candidateAppsuiteFileSetVersion;
        } catch (AppsuiteConfigurationException e) {
            exceptionThrown = true;
            throw e;
        } catch (ConfigurationException e) {
            exceptionThrown = true;
            throw new AppsuiteConfigurationException(appsuite, e.getMessage(), e.getCause());
        } catch (Exception e) {
            exceptionThrown = true;
            throw new AppsuiteConfigurationException(appsuite, "cannot apply configuration due to exception. " + e.getMessage(), e);
        } finally {
            if (exceptionThrown && candidateAppsuiteFileSetVersion != NO_FILESET) {
                LOGGER.info("appsuite[{}] fileset - exception is thrown deleting candidate filSet[{}]", appsuite, candidateAppsuiteFileSetVersion);
                versionedFileSet.delete(appsuitePath, candidateAppsuiteFileSetVersion);
            }
        }
    }

    @Override
    public void delete(String appsuite, int version) {
        String appsuitePath = getAppsuitePath(appsuite);
        try (Lock appsuiteLock = versionedFileSet.lock(appsuite, true)) {
            int effective = versionedFileSet.getEffective(appsuitePath);
            if (version == effective) {
                throw new CannotDeleteEffectiveFileSetException(appsuite, version);
            }
        }
    }

    @Override
    final public Lock lock(String name, boolean write) {
        return versionedFileSet.lock(name, write);
    }

    @Override
    public SortedSet<Integer> getVersions(String appsuite) {
        try (Lock appsuiteLock = versionedFileSet.lock(appsuite, false)) {
            return versionedFileSet.getVersions(getAppsuitePath(appsuite));
        }
    }

    @Override
    public SortedSet<String> getRelativePaths(String appsuite, int version) {
        try (Lock appsuiteLock = versionedFileSet.lock(appsuite, false)) {
            return versionedFileSet.getRelativePaths(getAppsuitePath(appsuite), version);
        }
    }

    @Override
    public void downloadItem(String appsuite, int version, String item, OutputStream target) {
        try (Lock appsuiteLock = versionedFileSet.lock(appsuite, false)) {
            versionedFileSet.downloadItem(getAppsuitePath(appsuite), version, item, target);
        }
    }


    @Override
    public void downloadFileSetAsZip(String appsuite, int version, OutputStream target) {
        try (Lock appsuiteLock = versionedFileSet.lock(appsuite, false)) {
            versionedFileSet.downloadFileSetAsZip(getAppsuitePath(appsuite), version, target);
        }
    }

    abstract protected void postInstall(String appsuite, int effective, int candidateAppsuiteFileSetVersion);

    abstract protected String getAppsuitePath(final String appsuite);
}
