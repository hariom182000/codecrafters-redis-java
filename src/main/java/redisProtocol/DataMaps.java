package redisProtocol;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataMaps {
    final Map<String, String> stringMap = new ConcurrentHashMap<>();
    final Map<String, String> keyDataTypeMap = new ConcurrentHashMap<>();
    final Map<String, Long> keyTtl = new ConcurrentHashMap<>();
    final Map<String, String> configMap = new ConcurrentHashMap<>();

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