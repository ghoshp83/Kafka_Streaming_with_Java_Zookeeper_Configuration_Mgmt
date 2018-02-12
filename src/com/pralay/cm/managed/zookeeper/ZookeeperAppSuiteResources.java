package com.pralay.cm.managed.zookeeper;

import com.pralay.cm.ConfigurationException;
import com.pralay.cm.managed.AppSuiteResources;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class ZookeeperAppSuiteResources implements AppSuiteResources {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperAppSuiteResources.class);

    private static final String APP_SUITE_ANY = "__ANY_APPSUITE__393481231232121__";
    private static final String APP_SUITE_KEY = "appSuite";

    private final CuratorFramework curatorFramework;

    public static final String ROOT = "/platform/resources";

    private static final Gson GSON = new Gson();

    @Inject
    public ZookeeperAppSuiteResources(CuratorFramework curatorFramework) {
        this.curatorFramework = curatorFramework;
    }

    @Override
    public void assignToAppSuite(String resourceType, String resourceId, String appsuite) {
        byte[] dataMeansAssigned = GSON.toJson(Collections.singletonMap(APP_SUITE_KEY, appsuite)).getBytes();
        try {
            try (ZookeeperLock writeLock = ZookeeperLock.write(curatorFramework, ROOT)) {
                curatorFramework.create()
                        .creatingParentsIfNeeded()
                        .forPath(
                                getResourcePath(resourceType, resourceId),
                                dataMeansAssigned);
                LOGGER.info("resource {}/{} is assigned to appsuite {}", resourceType, resourceId, appsuite);
            } catch (KeeperException.NodeExistsException e) {
                curatorFramework.setData()
                        .forPath(
                                getResourcePath(resourceType, resourceId),
                                dataMeansAssigned);
                LOGGER.info("resource {}/{} is reassigned to appsuite {}", resourceType, resourceId, appsuite);
            }
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    @Override
    public void detachFromAppSuite(String resourceType, String resourceId, String appsuite) {
        try (ZookeeperLock writeLock = ZookeeperLock.write(curatorFramework, ROOT)) {
            curatorFramework.setData().forPath(
                    getResourcePath(resourceType, resourceId),
                    GSON.toJson(Collections.emptyMap()).getBytes());
            LOGGER.info("resource {}/{} is detached from appsuite {}", resourceType, resourceId, appsuite);
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    @Override
    public Set<AppSuiteResource> getAllDetached() {
        return getResourcesAsSet(null, null);
    }

    @Override
    public Set<AppSuiteResource> getResources() {
        return getResourcesAsSet(APP_SUITE_ANY, null);
    }

    @Override
    public Set<AppSuiteResource> getResources(String appsuite) {
        return getResourcesAsSet(appsuite, null);
    }

    @Override
    public Set<String> getResources(String appsuite, String resourceType) {
        ImmutableSet.Builder<String> resultBuilder = ImmutableSet.builder();
        for (AppSuiteResource resource: getResourcesAsSet(appsuite, resourceType))
            resultBuilder.add(resource.getResourceId());
        return resultBuilder.build();
    }

    private Set<AppSuiteResource> getResourcesAsSet(String appsuite, String resourceType) {
        ImmutableSet.Builder<AppSuiteResource> resultBuilder = ImmutableSet.builder();
        Set<String> resourceTypes = resourceType == null ? getAvailableResources() : ImmutableSet.of(resourceType);
        try (ZookeeperLock readLock = ZookeeperLock.read(curatorFramework, ROOT)) {
            for (String rt : resourceTypes) {
                try {
                    for (String resource : curatorFramework.getChildren().forPath(getResourceTypeRoot(rt))) {
                        Map<String, Object> resourceData = GSON.fromJson(
                                new InputStreamReader(
                                        new ByteArrayInputStream(
                                                curatorFramework.getData().forPath(getResourcePath(rt, resource))
                                        )
                                ), Map.class);
                        String appsuiteStored = (String) resourceData.get(APP_SUITE_KEY);
                        if (
                                (appsuite == null && appsuiteStored == null) ||
                                        (APP_SUITE_ANY.equals(appsuite) && appsuiteStored != null) ||
                                        (appsuite != null && appsuite.equals(appsuiteStored))
                                ) {
                            resultBuilder.add(new AppSuiteResource(rt, resource, appsuiteStored));
                        }
                    }
                } catch (KeeperException.NoNodeException e) {
                    // do nothing - return empty set
                } catch (Exception e) {
                    throw new ConfigurationException(e);
                }
            }
        }
        return resultBuilder.build();
    }

    private String getResourcePath(String resourceType, String resourceId) {
        return getResourceTypeRoot(resourceType) + "/" + resourceId;
    }

    private String getResourceTypeRoot(String resourceType) {
        return ROOT + "/" + resourceType;
    }

    private Set<String> getAvailableResources() {
        try {
            return ImmutableSet.copyOf(curatorFramework.getChildren().forPath(ROOT));
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }
}
