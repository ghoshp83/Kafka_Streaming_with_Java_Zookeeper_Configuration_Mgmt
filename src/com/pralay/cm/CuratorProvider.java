package com.pralay.cm;

import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Init curator based zookeeper access by reusing AppConfiguration
 *
 */
public class CuratorProvider implements Provider<CuratorFramework> {
    private final ServiceAddressProvider zookeeperAddressProvider;

    @Inject
    public CuratorProvider(@Named(ServiceAddressProvider.ZOOKEEPER_SERVICE_QUALIFIER) final ServiceAddressProvider zookeeperAddressProvider) {
        this.zookeeperAddressProvider = zookeeperAddressProvider;
    }

    /**
     *
     * 1. Sets up proper zk address parameter from APPConfiguration or from System.getProperty
     * 2. starts {@link org.apache.curator.framework.CuratorFramework} with address and 3 retry times as parameters
     *
     * @return
     */
    @Override
    public CuratorFramework get() {
        List<String> addresses = zookeeperAddressProvider.getAddresses();
        if (addresses.isEmpty())
            throw new ResourceIsNotAvailable("no Zookeeper address is provided");

        String address = Joiner.on(',').join(addresses);

        int retryTimes = 3;
        int sleepMsBetweenRetries = 1000;
        CuratorFramework framework = CuratorFrameworkFactory.newClient(address, new RetryNTimes(retryTimes, sleepMsBetweenRetries));
        framework.start();
        try {
            if (!framework.blockUntilConnected(retryTimes * sleepMsBetweenRetries, TimeUnit.MILLISECONDS)) {
                throw new ConfigurationException("cannot connect to ZooKeeper address: " + address);
            }
        } catch (InterruptedException e) {
            throw new ConfigurationException("interrupted - cannot connect to ZooKeeper", e);
        }
        return framework;
    }
}
