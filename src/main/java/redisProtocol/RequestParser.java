package redisProtocol;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RequestParser {
    private BufferedWriter writer;
    private BufferedReader reader;
    private DataMaps dataMaps;
    private final List<Object> commands = new ArrayList<>();
    private final List<OperationDetail> operationDetails = new ArrayList<>();
    private int commnadSize = 0;

    public RequestParser(final BufferedWriter writer, final BufferedReader reader, final DataMaps dataMaps) {
        this.reader = reader;
        this.writer = writer;
        this.dataMaps = dataMaps;
    }


    public void help() throws IOException {
        String content;
        while (true) {
            content = reader.readLine();
            if (Objects.isNull(content) || content.isEmpty() || content.isBlank()) {
                break;
            }
            System.out.println("timestamp ::: " + System.currentTimeMillis());
            System.out.println("message ::" + content);
            if (content.charAt(0) == '*' && commnadSize == 0) {   // commandSize==0 is a hack, array length begins with *, but key matching also has *
                commnadSize = parseInteger(content);
                //operationDetails.add(new OperationDetail(Operation.ARRAY, content.substring(1)));
            } else if (content.charAt(0) == '$') {
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
                parseData(content);
            }
            if (commands.size() == commnadSize) processLastCommand();
        }
    }

    private void processLastCommand() throws IOException {
        if (commands.isEmpty()) return;
        if ("PING".equalsIgnoreCase((String) commands.get(0))) {
            handlePingCommand();
        } else if ("GET".equalsIgnoreCase((String) commands.get(0))) {
            handleGetCommand();
        } else if ("SET".equalsIgnoreCase((String) commands.get(0))) {
            handleSetCommand();
        } else if ("ECHO".equalsIgnoreCase((String) commands.get(0))) {
            handleEchoCommand();
        } else if ("CONFIG".equalsIgnoreCase((String) commands.get(0))) {
            handleConfigCommands();
        } else if ("KEYS".equalsIgnoreCase((String) commands.get(0))) {
            handleKeysCommand();
        }
        operationDetails.clear();
        commnadSize = 0;
        commands.clear();

    }

    private void handleKeysCommand() {
        System.out.println("handling Keys *");
        if ("*".equalsIgnoreCase((String) commands.get(1))) {
            dataMaps.getStringMap().forEach((key, value) -> {
                try {
                    writer.write(getKeyValueBulkString(key, value));
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
                try {
                    writer.flush();
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private String getKeyValueBulkString(final String key, final String value) {
        return "*2\r\n$" + key.length() + "\r\n" + key + "\r\n$" + value.length() + "\r\n" + value + "\r\n";
    }

    private void handleConfigCommands() throws IOException {
        if ("GET".equalsIgnoreCase((String) commands.get(1))) {
            final String key = (String) commands.get(2);
            final String value = dataMaps.getConfigMap().get(key);
            writer.write(getKeyValueBulkString(key, value));
            writer.flush();
        }
    }

    private void handleGetCommand() throws IOException {
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


    private void handleSetCommand() throws IOException {
        final String key = (String) commands.get(1);
        final String value = (String) commands.get(2);
        Long ttl = (long) -1;
        if (commands.size() > 3) {
            String px = (String) commands.get(3);
            if ("px".equalsIgnoreCase(px)) {
                ttl = Long.parseLong((String) commands.get(4));
            }
        }
        synchronized (key) {
            final Long systemTime = System.currentTimeMillis();
            dataMaps.getStringMap().put(key, value);
            if (ttl > 0) dataMaps.getKeyTtl().put(key, systemTime + ttl);
        }
        writer.write("+OK\r\n");
        writer.flush();
    }

    private void handleEchoCommand() throws IOException {
        final String value = (String) commands.get(1);
        writer.write("$" + value.length() + "\r\n" + value + "\r\n");
        writer.flush();
    }

    private void handlePingCommand() throws IOException {
        writer.write("$4\r\nPONG\r\n");
        writer.flush();
    }

    private String parseSimpleString(final String content) {
        return content.substring(1);
    }

    private Integer parseInteger(final String content) {
        return Integer.parseInt(content.substring(1));
    }


    private void parseData(final String content) throws IOException {
        final OperationDetail operationDetail = operationDetails.getLast();
        if (Operation.BULK_STRING.equals(operationDetail.getOperation())) {
            if (operationDetail.getValue().equals(String.valueOf(content.length()))) {
                commands.add(content);
            } else throw new RuntimeException();
        }
    }
}