package redisProtocol;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataMaps {
    final private Map<String, String> stringMap = new ConcurrentHashMap<>();
    final private Map<String, String> keyDataTypeMap = new ConcurrentHashMap<>();
    final private Map<String, Long> keyTtl = new ConcurrentHashMap<>();
    final private Map<String, String> configMap = new ConcurrentHashMap<>();

    public Map<String, String> getConfigMap() {
        return configMap;
    }

    public Map<String, Long> getKeyTtl() {
        return keyTtl;
    }

    public Map<String, String> getStringMap() {
        return stringMap;

    }

    public Map<String, String> getKeyDataTypeMap() {
        return keyDataTypeMap;
    }
}