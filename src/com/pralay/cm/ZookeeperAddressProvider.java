package com.pralay.cm;

import com.pralay.core.configuration.ExecutionConfiguration;
import com.pralay.core.configuration.ServerAddr;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;

/**
 * Init curator based zookeeper access by reusing executionConfiguration
 *
 */
public class ZookeeperAddressProvider extends ServiceAddressProvider {
    private final ExecutionConfiguration executionConfiguration;

    @Inject
    public ZookeeperAddressProvider(final ExecutionConfiguration executionConfiguration) {
        this.executionConfiguration = executionConfiguration;
    }

    @Override
    public List<String> getAddresses() {
        String address = System.getProperty(ServiceAddressProvider.getAddressKey(ZOOKEEPER_SERVICE_QUALIFIER));

        ImmutableList<String> hostList = address != null ?
                ImmutableList.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(address)) :
                ImmutableList.<String>of();

        if (hostList.isEmpty()) {
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            for (ServerAddr addr: executionConfiguration.getZookeeperHosts()) {
                builder.add(addr.getHost() + ":" + addr.getPort());
            }
            hostList = builder.build();
        }
        return hostList;
    }
}
