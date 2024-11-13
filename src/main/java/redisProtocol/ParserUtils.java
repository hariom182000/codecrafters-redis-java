package redisProtocol;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class ParserUtils {

    final static Set<String> commandsToBePropagated = Set.of("SET");

    public static void readCommand(final String content, final List<OperationDetail> operationDetails, final List<Object> commands
    ) throws IOException {
        if (content.charAt(0) == '$') {
            operationDetails.add(new OperationDetail(Operation.BULK_STRING, content.substring(1)));
        } else if (content.charAt(0) == '+') {
            operationDetails.add(new OperationDetail(Operation.SIMPLE_STRING, null));
            commands.add(parseSimpleString(content));
        } else if (content.charAt(0) == ':') {
            operationDetails.add(new OperationDetail(Operation.INTEGER, null));
            commands.add(parseInteger(content));
        } else if (content.charAt(0) == '_') {
            operationDetails.add(new OperationDetail(Operation.NULL, null));
        } else if (content.charAt(0) == '#') {
            operationDetails.add(new OperationDetail(Operation.BOOLEAN, null));
        } else if (content.charAt(0) == ',') {
            operationDetails.add(new OperationDetail(Operation.DOUBLE, null));
        } else {
            parseData(content, operationDetails, commands);
        }
    }

    public static String parseSimpleString(final String content) {
        return content.substring(1);
    }

    public static Integer parseInteger(final String content) {
        return Integer.parseInt(content.substring(1));
    }

    public static void parseData(final String content, final List<OperationDetail> operationDetails, final List<Object> commands) throws IOException {
        final OperationDetail operationDetail = operationDetails.getLast();
        if (Operation.BULK_STRING.equals(operationDetail.getOperation())) {
            if (operationDetail.getValue().equals(String.valueOf(content.length()))) {
                commands.add(content);
            } else throw new RuntimeException();
        }
    }


    public static void processLastCommand(final List<Object> commands, final BufferedWriter writer, final DataMaps dataMaps, final OutputStream out, final Set<OutputStream> replicaConnections) throws IOException {
        if (Objects.isNull(commands) || commands.isEmpty()) return;
        if ("PING".equalsIgnoreCase((String) commands.get(0))) {
            handlePingCommand(writer);
        } else if ("GET".equalsIgnoreCase((String) commands.get(0))) {
            handleGetCommand(writer, commands, dataMaps);
        } else if ("SET".equalsIgnoreCase((String) commands.get(0))) {
            handleSetCommand(commands, dataMaps, writer);
        } else if ("ECHO".equalsIgnoreCase((String) commands.get(0))) {
            handleEchoCommand(commands, writer);
        } else if ("CONFIG".equalsIgnoreCase((String) commands.get(0))) {
            handleConfigCommands(commands, dataMaps, writer);
        } else if ("KEYS".equalsIgnoreCase((String) commands.get(0))) {
            handleKeysCommand(commands, dataMaps, writer);
        } else if ("INFO".equalsIgnoreCase((String) commands.get(0))) {
            handleInfoCommand(commands, dataMaps, writer);
        } else if ("REPLCONF".equalsIgnoreCase((String) commands.get(0))) {
            handleReplConfCommand(writer);
        } else if ("PSYNC".equalsIgnoreCase((String) commands.get(0))) {
            handlePsyncCommand(dataMaps, writer);
            sendRdbFile(out);
            replicaConnections.add(out);
        }
        commands.clear();
    }

    public static void propagateToReplicas(final String request, final List<Object> commands, final Set<OutputStream> replicaConnections) throws IOException {
        if (commandsToBePropagated.contains((String) commands.get(0))) {
            for (OutputStream out : replicaConnections) {
                out.write(request.getBytes());
                out.flush();
            }
        }
    }

    public static void handlePsyncCommand(final DataMaps dataMaps, final BufferedWriter writer) throws IOException {
        writer.write("+FULLRESYNC " + dataMaps.getConfigMap().get("master_replid") + " 0\r\n");
        writer.flush();
    }

    public static void sendRdbFile(final OutputStream writer) throws IOException {
        String fileContents =
                "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
        byte[] bytes = Base64.getDecoder().decode(fileContents);
        writer.write(("$" + bytes.length + "\r\n").getBytes());
        writer.write(bytes);
        writer.flush();
    }


    public static void handleReplConfCommand(final BufferedWriter writer) throws IOException {
        writer.write("+OK\r\n");
        writer.flush();
    }

    public static void handleInfoCommand(final List<Object> commands, final DataMaps dataMaps, final BufferedWriter writer) throws IOException {
        if ("replication".equalsIgnoreCase((String) commands.get(1))) {
            if (dataMaps.getConfigMap().containsKey("replicaof")) {
                writer.write("$10\r\nrole:slave\r\n");
            } else {
                writer.write(getInfoValue(dataMaps));
            }
        }
        writer.flush();
    }

    public static String getInfoValue(final DataMaps dataMaps) {
        String data = "role:master";
        Long length = 11L;
        if (dataMaps.getConfigMap().containsKey("master_repl_offset")) {
            data += "\r\nmaster_repl_offset:" + dataMaps.getConfigMap().get("master_repl_offset");
            length += "master_repl_offset:".length() + dataMaps.getConfigMap().get("master_repl_offset").length() + 2;
        }
        if (dataMaps.getConfigMap().containsKey("master_replid")) {
            data += "\r\nmaster_replid:" + dataMaps.getConfigMap().get("master_replid");
            length += "master_replid:".length() + dataMaps.getConfigMap().get("master_replid").length() + 2;
        }
        System.out.println("data with length is " + length + "  " + data);
        return "$" + length + "\r\n" + data + "\r\n";
    }

    public static void handleKeysCommand(final List<Object> commands, final DataMaps dataMaps, final BufferedWriter writer) throws IOException {
        if ("*".equalsIgnoreCase((String) commands.get(1))) {
            StringBuilder res = new StringBuilder();
            res.append("*").append(dataMaps.getStringMap().size()).append("\r\n");
            dataMaps.getStringMap().keySet().forEach(key -> {
                res.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
            });
            System.out.println("keys * resposne is " + res);
            writer.write(res.toString());
            writer.flush();
        }
    }

    public static String getKeyValueBulkString(final String key, final String value) {
        return "*2\r\n$" + key.length() + "\r\n" + key + "\r\n$" + value.length() + "\r\n" + value + "\r\n";
    }

    public static Boolean writeNullIfEmptyMap(final DataMaps dataMaps, final BufferedWriter writer) throws IOException {
        if (dataMaps.getStringMap().isEmpty()) {
            writer.write("$-1\r\n");
            writer.flush();
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    public static void handleConfigCommands(final List<Object> commands, final DataMaps dataMaps, final BufferedWriter writer) throws IOException {
        if ("GET".equalsIgnoreCase((String) commands.get(1))) {
            final String key = (String) commands.get(2);
            final String value = dataMaps.getConfigMap().get(key);
            writer.write(getKeyValueBulkString(key, value));
            writer.flush();
        }
    }

    public static void handleGetCommand(final BufferedWriter writer, final List<Object> commands, final DataMaps dataMaps) throws IOException {
        if (writeNullIfEmptyMap(dataMaps, writer)) return;
        final String key = (String) commands.get(1);
        if (dataMaps.getStringMap().containsKey(key)) {
            final String data = dataMaps.getStringMap().get(key);
            if (dataMaps.getKeyTtl().containsKey(key)) {
                if (System.currentTimeMillis() >= dataMaps.getKeyTtl().get(key)) {
                    writer.write("$-1\r\n");
                } else {
                    writer.write("$" + data.length() + "\r\n" + data + "\r\n");
                }
            } else {
                writer.write("$" + data.length() + "\r\n" + data + "\r\n");
            }
        } else {
            writer.write("$-1\r\n");

        }
        writer.flush();
    }


    public static void handleSetCommand(final List<Object> commands, final DataMaps dataMaps, final BufferedWriter writer) throws IOException {
        final String key = (String) commands.get(1);
        final String value = (String) commands.get(2);
        Long ttl = (long) -1;
        if (commands.size() > 3) {
            String px = (String) commands.get(3);
            if ("px".equalsIgnoreCase(px)) {
                ttl = Long.parseLong((String) commands.get(4));
            }
        }
        final Long systemTime = System.currentTimeMillis();
        synchronized (key) {   // to make the following map insertions atomic
            dataMaps.getStringMap().put(key, value);
            if (ttl > 0) dataMaps.getKeyTtl().put(key, systemTime + ttl);
        }
        if (dataMaps.getReplica()) return;
        writer.write("+OK\r\n");
        writer.flush();
    }

    public static void handleEchoCommand(final List<Object> commands, final BufferedWriter writer) throws IOException {
        final String value = (String) commands.get(1);
        writer.write("$" + value.length() + "\r\n" + value + "\r\n");
        writer.flush();
    }

    public static void handlePingCommand(final BufferedWriter writer) throws IOException {
        writer.write("$4\r\nPONG\r\n");
        writer.flush();
    }
}
