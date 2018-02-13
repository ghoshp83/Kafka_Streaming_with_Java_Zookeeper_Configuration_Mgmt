package com.pralay.cm.managed;

public interface ConfigInstallFromResources {
    /**
     * Installs list configurations from resource into config store.
     *
     * THIS IS A TEMPORARY FUNCTIONALITY! Only valid till app-manager supports configuration store.
     *
     * @param resources
     * @return effective configuration
     */
    int installFromResources(String... resources);
}
