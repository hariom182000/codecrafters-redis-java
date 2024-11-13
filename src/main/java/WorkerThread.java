import redisProtocol.DataMaps;
import redisProtocol.ParserUtils;
import redisProtocol.Parser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.List;
import java.util.Set;

public class WorkerThread implements Runnable {

    private Socket clientSocket;
    private DataMaps dataMaps;
    private Set<OutputStream> replicaConnections;

    public WorkerThread(final Socket clientSocket, final DataMaps dataMaps, final Set<OutputStream> replicaConnections) {
        this.clientSocket = clientSocket;
        this.dataMaps = dataMaps;
        this.replicaConnections = replicaConnections;
    }


    @Override
    public void run() {
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()))) {
            final Parser requestParser = new Parser(reader);
            while (true) {

                final List<Object> commands = requestParser.help();
                ParserUtils.processLastCommand(commands, writer, dataMaps, clientSocket.getOutputStream(), replicaConnections);
                ParserUtils.propagateToReplicas( commands, replicaConnections);
                commands.clear();
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
