import redisProtocol.DataMaps;
import redisRDB.ReadRDBFile;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) throws IOException {

        final DataMaps dataMaps = new DataMaps();

        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                dataMaps.getConfigMap().put(args[i].substring(2), args[i + 1]);
                i++;
            }
        }
        final ReadRDBFile readRDBFile = new ReadRDBFile(dataMaps);
        readRDBFile.read();
        int port = 6379;
        if (dataMaps.getConfigMap().containsKey("port")) port = Integer.parseInt(dataMaps.getConfigMap().get("key"));

        try (final ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            while (serverSocket.isBound() && !serverSocket.isClosed()) {
                final Socket clientSocket = serverSocket.accept();
                final Thread t = new Thread(new WorkerThread(clientSocket, dataMaps));
                t.start();
            }
        } catch (final IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }


}



