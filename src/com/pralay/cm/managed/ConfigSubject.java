package com.pralay.cm.managed;

public interface ConfigSubject  {

    /**
     * Attaches a config observer
     * observer's {@link ConfigObserver#update(com.pralay.cm.managed.apply.ConfigDiff)} is called during {@link #apply(String, int, int)}
     *
     * @param observer
     */
    void attach(ConfigObserver observer);

    /**
     * Detaches the config observer
     *
     * @param observer
     */
    void detach(ConfigObserver observer);
}
