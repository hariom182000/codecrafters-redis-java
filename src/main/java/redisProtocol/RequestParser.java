package redisProtocol;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Objects;
import java.util.Stack;

public class RequestParser {
    private BufferedWriter writer;
    private BufferedReader reader;
    private DataMaps dataMaps;
    private final Stack<String> commands = new Stack<>();
    private final Stack<OperationDetail> operationDetails = new Stack<>();

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
            if (content.charAt(0) == '*') {
                operationDetails.push(new OperationDetail(Operation.ARRAY, content.substring(1)));
            } else if (content.charAt(0) == '$') {
                operationDetails.push(new OperationDetail(Operation.BULK_STRING, content.substring(1)));
            } else if (content.charAt(0) == '+') {
                understandCommand(parseSimpleString(content));
            } else if (content.charAt(0) == ':') {
                understandCommand(parseInteger(content));
            } else if (content.charAt(0) == '_') {
                operationDetails.push(new OperationDetail(Operation.NULL, null));
            } else if (content.charAt(0) == '#') {
                operationDetails.push(new OperationDetail(Operation.BOOLEAN, null));
            } else if (content.charAt(0) == ',') {
                operationDetails.push(new OperationDetail(Operation.DOUBLE, null));
            } else {
                parseData(content);
            }
        }
    }

    private void understandCommand(final Object value) throws IOException {
        final String command = commands.pop();
        System.out.println("reading in commands " + command);
        if ("ECHO".equalsIgnoreCase(command) && Objects.nonNull(value)) {
            writer.write("$" + value.toString().length() + "\r\n" + value.toString() + "\r\n");
            writer.flush();
        } else if ("SET".equalsIgnoreCase(command)) {
            reader.readLine();
            final String data = reader.readLine();
            dataMaps.getStringMap().put(value.toString(), data);
            writer.write("+OK\r\n");
            writer.flush();
        } else if ("GET".equalsIgnoreCase(command)) {
            final String key = value.toString();
            if (dataMaps.getStringMap().containsKey(key)) {
                final String data = dataMaps.getStringMap().get(key);
                writer.write("$" + data.length() + "\r\n" + data + "\r\n");
                writer.flush();
            } else {
                writer.write("$-1\r\n");
                writer.flush();
            }

        }
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
        final OperationDetail operationDetail = operationDetails.pop();
        if (Operation.BULK_STRING.equals(operationDetail.getOperation())) {
            if (operationDetail.getValue().equals(String.valueOf(content.length()))) {
                System.out.println("pushing in commands" + content);
                if (commands.isEmpty()) {
                    if ("PING".equalsIgnoreCase(content)) handlePingCommand();
                    else commands.push(content);
                } else understandCommand(content);
            } else throw new RuntimeException();
        }
    }
}
