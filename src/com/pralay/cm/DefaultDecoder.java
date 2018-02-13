package com.pralay.cm;

import com.google.gson.Gson;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;

/**
 * Default decoder with supporting following types
 * - basic types
 * - InputStream for URL values
 * - JSON value to object
 */
public class DefaultDecoder implements Decoder {
    private static final Gson gson = new Gson();

    @SuppressWarnings("unchecked")
    @Override
    public <T> T decode(Class<T> type, String encoded) {

        if (encoded == null) {
            return null;
        }
        // Try primitives first
        if (type.equals(String.class)) {
            return (T) encoded;
        }
        else if (type.equals(int.class) || type.equals(Integer.class)) {
            return (T) Integer.valueOf(encoded);
        }
        else if (type.equals(long.class) || type.equals(Long.class)) {
            return (T) Long.valueOf(encoded);
        }
        else if (type.equals(short.class) || type.equals(Short.class)) {
            return (T) Short.valueOf(encoded);
        }
        else if (type.equals(double.class) || type.equals(Double.class)) {
            return (T) Double.valueOf(encoded);
        }
        else if (type.equals(float.class) || type.equals(Float.class)) {
            return (T) Float.valueOf(encoded);
        }
        else if (type.equals(BigInteger.class)) {
            return (T) new BigInteger(encoded);
        }
        else if (type.equals(BigDecimal.class)) {
            return (T) new BigDecimal(encoded);
        } else if (type.equals(InputStream.class)) {
            URL url = APPConfigurations.getURLFromResource(null, encoded);
            try {
                return type.cast(url != null ? url.openStream() : null);
            } catch (IOException e) {
                throw new ConfigurationException(e);
            }
        } else {
            return (T)gson.fromJson(encoded, type);
        }
    }
}
