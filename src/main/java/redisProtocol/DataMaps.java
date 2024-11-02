package redisProtocol;

import java.util.HashMap;
import java.util.Map;

public class DataMaps {
    final Map<String, String> stringMap = new HashMap<>();
    final Map<String, String> keyDataTypeMap = new HashMap<>();

    public Map<String, String> getStringMap() {
        return stringMap;
    }

    public Map<String, String> getKeyDataTypeMap() {
        return keyDataTypeMap;
    }
}
