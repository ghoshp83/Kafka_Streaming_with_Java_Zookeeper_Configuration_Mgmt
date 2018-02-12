package com.pralay.cm.managed;

import com.pralay.cm.AppsuiteConfigurationException;

public class ConcurrentModificationException extends AppsuiteConfigurationException {
    private final int candidate, effective;

    public ConcurrentModificationException(String name, int candidate, int effective) {
        super(name, "concurrent [" + name + "] modification from effective[" + effective +"] to candidate[" + candidate + "]");
        this.candidate = candidate;
        this.effective = effective;
    }

    public int getCandidate() {
        return candidate;
    }

    public int getEffective() {
        return effective;
    }
}
