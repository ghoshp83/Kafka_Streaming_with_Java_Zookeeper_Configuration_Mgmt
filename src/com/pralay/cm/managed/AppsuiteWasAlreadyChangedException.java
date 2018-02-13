package com.pralay.cm.managed;

import com.pralay.cm.AppsuiteConfigurationException;

public class AppsuiteWasAlreadyChangedException extends AppsuiteConfigurationException {
    public AppsuiteWasAlreadyChangedException(String appsuite, int candidate, int effective) {
        super(appsuite, "concurrent appsuite[" + appsuite + "] modification from effective[" + effective +"] to candidate[" + candidate + "]");
    }
}
