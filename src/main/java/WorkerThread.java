import redisProtocol.DataMaps;
import redisProtocol.ParserUtils;
import redisProtocol.Parser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.List;

public class WorkerThread implements Runnable {

    private Socket clientSocket;
    private DataMaps dataMaps;

    public WorkerThread(final Socket clientSocket, final DataMaps dataMaps) {
        this.clientSocket = clientSocket;
        this.dataMaps = dataMaps;
    }


    @Override
    public void run() {
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()))) {
            final Parser requestParser = new Parser(reader);
            while (true) {
                final List<Object> commands = requestParser.help();
                ParserUtils.processLastCommand(commands, writer, dataMaps);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
