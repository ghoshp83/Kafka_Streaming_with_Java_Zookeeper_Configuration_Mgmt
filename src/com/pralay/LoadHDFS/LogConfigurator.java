package com.pralay.LoadHDFS;

import com.google.common.base.Throwables;
import org.apache.log4j.xml.DOMConfigurator;

public class LogConfigurator {

    public static void configure(String log4jConfigFilename) {
        final String LOG_PATH = "LOG_PATH";
        final long DELAY = 60000L;

        // Set LOG_DIRS environment variable value as LOG_PATH
        final String ENV_LOG_DIRS = System.getenv("LOG_DIRS");
        if (ENV_LOG_DIRS != null && System.getProperty(LOG_PATH) == null) {
            System.out.println(String.format("Setting [%s] system property to %s", LOG_PATH, ENV_LOG_DIRS));
            System.setProperty(LOG_PATH, ENV_LOG_DIRS);
        }

        try {
            System.out.println("Configuring log4j watch, for " + log4jConfigFilename + " with delay=" + DELAY);
            DOMConfigurator.configureAndWatch(log4jConfigFilename, DELAY);
        } catch (Exception e) {
            System.out.println("Could not set and configure log4j! " + Throwables.getStackTraceAsString(e));
        }
    }
}
