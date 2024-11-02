import redisProtocol.DataMaps;
import redisProtocol.RequestParser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

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
            final RequestParser requestParser = new RequestParser(writer, reader,dataMaps);
            requestParser.help();
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
