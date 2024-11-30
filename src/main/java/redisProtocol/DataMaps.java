package redisProtocol;

import java.net.Socket;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class DataMaps {
    final private Map<String, String> stringMap = new ConcurrentHashMap<>();
    final private Map<String, String> keyDataTypeMap = new ConcurrentHashMap<>();
    final private Map<String, Long> keyTtl = new ConcurrentHashMap<>();
    final private Map<String, String> configMap = new ConcurrentHashMap<>();
    private Boolean isReplica = Boolean.FALSE;
    final private AtomicLong offset = new AtomicLong(0);
    final private AtomicLong bytesSentToReplicas = new AtomicLong(0);
    final private Set<Socket> replicaConnections = new HashSet<>();

    public Boolean getReplica() {
        return isReplica;
    }

    public void setReplica(Boolean replica) {
        isReplica = replica;
    }

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

    public AtomicLong getOffset() {
        return offset;
    }

    public synchronized void increaseOffset(final long offset) {
        this.offset.addAndGet(offset);

    }

    public AtomicLong getBytesSentToReplicas() {
        return bytesSentToReplicas;
    }

    public void increaseBytesSentToReplicas(final long bytesSentToReplicas) {
        this.bytesSentToReplicas.addAndGet(bytesSentToReplicas);
    }

    public Set<Socket> getReplicaConnections() {
        return replicaConnections;
    }

    public void addReplica(Socket replicaConnection) {
        this.replicaConnections.add(replicaConnection);
    }
}