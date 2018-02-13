package com.pralay.cm;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.netflix.config.ConcurrentMapConfiguration;
import com.netflix.config.DynamicURLConfiguration;
import com.netflix.config.sources.URLConfigurationSource;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.JNDIConfiguration;
import org.apache.commons.configuration.MapConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Closed class for providing factory methods for URL based configuration sources.
 *
 */
abstract public class APPConfigurations {
    private static final Logger LOGGER = LoggerFactory.getLogger(com.pralay.cm.AppConfigManager.class);
    public static final String ENCODING = "UTF-8";

    private static final Pattern URL_PATTERN = Pattern.compile("^([\\p{Lower}]+)://([^/\\s]+)(/[^#?\\s]+)[#?]?(.*)$");

    private APPConfigurations() {
    }

    public static AbstractConfiguration emptyConfig() {
        return new MapConfiguration(Collections.emptyMap());
    }

    public static URL getURLFromResource(final ClassLoader l, final String resourceName) {
        URL url = null;
        // attempt to load from the context classpath
        ClassLoader loader = getClassLoader(l);

        if (loader != null) {
            url = loader.getResource(resourceName);
        }
        if (url == null) {
            // attempt to load from the system classpath
            url = ClassLoader.getSystemResource(resourceName);
        }
        if (url == null) {
            try {
                url = new URL(resourceName);
            } catch (MalformedURLException e) {
                try {
                    url = (new File(URLDecoder.decode(resourceName, ENCODING))).toURI().toURL();
                } catch (Exception ex) {
                    url = null;
                }
            }
        }
        return url;
    }

    public static AbstractConfiguration getConfigFromURL(final URL url) {
        try {
            return new ConcurrentMapConfiguration(
                    new URLConfigurationSource(url)
                            .poll(true, null)
                            .getComplete()
            );
        } catch (IOException e) {
            throw new IllegalArgumentException("configuration - cannot instantiate configuration from resource name " + url + " " + e.getMessage(), e);
        }
    }

    public static AbstractConfiguration getConfigFromResource(String resource) {
        ParsedURL parsedURL = parseResource(resource);
        if ("jndi".equals(parsedURL.protocol)) {
            try {
                return new JNDIConfiguration(new InitialContext(), parsedURL.path);
            } catch (NamingException e) {
                throw new IllegalArgumentException("configuration - cannot instantiate configuration from resource name " + resource + " " + e.getMessage(), e);
            }
        } else if (pollMillisec(parsedURL) != null) {
            // URL poll support
            return new DynamicURLConfiguration(0, pollMillisec(parsedURL), true, resource);
        } else if (parsedURL.url != null) {
            // read once
                return getConfigFromURL(parsedURL.url);
        }
        throw new IllegalArgumentException("configuration - cannot instantiate configuration from resource name " + resource);
    }

    static Integer pollMillisec(ParsedURL url) {
        try {
            return url.query.containsKey("poll") ? new Integer(url.query.get("poll")) : null;
        } catch(NumberFormatException e) {
            LOGGER.error("cannot parse poll parameter[{}]", url);
            return null;
        }
    }

    static ParsedURL parseResource(String resource) {
        ParsedURL parsedURL;
        Matcher m = URL_PATTERN.matcher(resource);
        if (m.matches()) {
            parsedURL = new ParsedURL(
                    resource,
                    m.group(1),
                    m.group(2),
                    m.group(3),
                    m.group(4) != null ? getQueryParamsMap(m.group(4)) : Collections.<String,String>emptyMap());
        } else {
            URL url = getURLFromResource(null, resource);
            parsedURL = new ParsedURL(
                    resource,
                    url.getProtocol(),
                    url.getHost() + (url.getPort() > 0 ? ":" + url.getPort() : ""),
                    url.getPath(),
                    url.getQuery() != null ? getQueryParamsMap(url.getQuery()) : Collections.<String,String>emptyMap());
            parsedURL.url = url;
        }
        return parsedURL;
    }


    public static Map<String, String> getQueryParamsMap(final String query) {
        if (query == null || query.isEmpty())
            return Collections.emptyMap();

        ImmutableMap.Builder<String, String> queryPairs = ImmutableMap.builder();
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            try {
                queryPairs.put(URLDecoder.decode(pair.substring(0, idx), ENCODING), URLDecoder.decode(pair.substring(idx + 1), ENCODING));
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("encoding " + ENCODING + " is not supported " + e.getMessage(), e);
                return Collections.emptyMap();
            }
        }
        return queryPairs.build();
    }


    private static ClassLoader getClassLoader(ClassLoader l) {
        return l != null ? l : Thread.currentThread().getContextClassLoader();
    }

    public static Properties getProperties(Configuration c) {
        Properties p = new Properties();
        for (Iterator i = c.getKeys(); i.hasNext();) {
            String name = (String) i.next();
            String value = c.getString(name);
            p.put(name, value);
        }
        return p;
    }

    public static MapDifference<String, String> diff(Configuration left, Configuration right) {
        return Maps.difference(
                Maps.fromProperties(getProperties(left)),
                Maps.fromProperties(getProperties(right))
        );
    }


    static class ParsedURL {
        public final String full;
        public final String protocol;
        public final String address;
        public final String path;
        public final Map<String,String> query;

        private URL url = null;

        ParsedURL(String full, String protocol, String address, String path, Map<String, String> query) {
            this.full = full;
            this.protocol = protocol;
            this.address = address;
            this.path = path;
            this.query = query;
        }

        public URL getUrl() {
            return url;
        }

        public boolean isClassPathResource() {
            return protocol == null || "file".equals(protocol) && !full.startsWith(protocol);
        }

        @Override
        public String toString() {
            return full;
        }
    }
}
