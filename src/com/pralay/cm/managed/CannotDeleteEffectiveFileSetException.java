package com.pralay.cm.managed;

import com.pralay.cm.ConfigurationException;

public class CannotDeleteEffectiveFileSetException extends ConfigurationException {
    public CannotDeleteEffectiveFileSetException(final String name, int version) {
        super("cannot delete effective fileset[" + name + "] version[" + version + "]");
    }
}
