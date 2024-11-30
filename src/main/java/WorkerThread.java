import redisProtocol.DataMaps;
import redisProtocol.ParserUtils;
import redisProtocol.Parser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

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
            System.out.println("Listening to users......");
            final Parser requestParser = new Parser(reader);
            List<Object> commands = new ArrayList<>();
            while (clientSocket.isBound() && !clientSocket.isClosed()) {
                commands = requestParser.help();
                ParserUtils.processLastCommand(commands, writer, dataMaps, clientSocket, false);
                ParserUtils.propagateToReplicas(commands, dataMaps);
                if (Objects.nonNull(commands)) commands.clear();
            }
        } catch (Exception e) {
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
