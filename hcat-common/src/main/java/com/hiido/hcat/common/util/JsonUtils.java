/**
 * May 31, 2013
 */
package com.hiido.hcat.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * @author lin
 * 
 */
public final class JsonUtils {
    private static final ObjectMapper jsonMapper = new ObjectMapper();
    static {
        jsonMapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    }

    private JsonUtils() {
    }

    public static <T> T jsonFromInput(InputStream in, String charset, Class<T> clazz) throws IOException,
            JsonProcessingException {
        synchronized (jsonMapper) {
            Reader reader = new InputStreamReader(in, charset);
            return jsonMapper.readValue(reader, clazz);
        }
    }

    public static String jsonString(Object obj) throws IOException, JsonProcessingException {
        synchronized (jsonMapper) {
            return jsonMapper.writeValueAsString(obj);
        }
    }

    public static <T> T jsonFrom(String str, Class<T> clazz) throws IOException, JsonProcessingException {
        synchronized (jsonMapper) {
            return jsonMapper.readValue(str, clazz);
        }
    }
}
