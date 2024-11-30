package redisProtocol;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Parser {
    private BufferedReader reader;

    public Parser(final BufferedReader reader) {
        this.reader = reader;
    }

    public List<Object> help() throws IOException {
        final List<Object> commands = new ArrayList<>();
        final List<OperationDetail> operationDetails = new ArrayList<>();
        Integer commnadSize = 0;
        String request = "";
        while (true) {
            final String content = reader.readLine();
            request += content + "\r\n";
            if (Objects.isNull(content) || content.isEmpty() || content.isBlank()) {
                break;
            }
            if (content.charAt(0) == '*' && commnadSize == 0) {   // commandSize==0 is a hack, array length begins with *, but key matching also has *
                commnadSize = ParserUtils.parseInteger(content);
                continue;
            } else if (content.charAt(0) == '+') {
                commnadSize = 1;
            } else if (content.charAt(0) == '$' && commnadSize == 0) {  // again a hack for commands which are not array (bulkStrings like pong)
                commnadSize = 1;
            } else if (content.charAt(0) == ':') { // again a hack for commands parsing int
                commnadSize = 1;
            }
            System.out.println("message ::" + content);
            ParserUtils.readCommand(content, operationDetails, commands);
            if (commands.size() == commnadSize) {
                operationDetails.clear();
                commands.add(request);
                return commands;
            }
        }
        return commands;
    }
}