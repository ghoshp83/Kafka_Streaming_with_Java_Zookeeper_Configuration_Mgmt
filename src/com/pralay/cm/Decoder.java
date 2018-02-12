package com.pralay.cm;

/**
 * Config value decoder which resolves types for {@link APPConfiguration#get}
 *
 */
public interface Decoder {
    <T> T decode(Class<T> type, String encoded);
}
