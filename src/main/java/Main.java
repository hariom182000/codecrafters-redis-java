import redisProtocol.DataMaps;
import redisRDB.ReadRDBFile;
import replication.Handshake;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

public class Main {
    public static void main(String[] args) throws IOException {

        final DataMaps dataMaps = new DataMaps();

        final Set<OutputStream> replicaConnections = new HashSet<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                System.out.println("arguments are :: " + args[i] + "  " + args[i + 1]);
                dataMaps.getConfigMap().put(args[i].substring(2), args[i + 1]);
                i++;
            }
        }
        if (!dataMaps.getConfigMap().containsKey("replicaof")) {
            dataMaps.getConfigMap().put("master_replid", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
            dataMaps.getConfigMap().put("master_repl_offset", "0");
        } else {
          dataMaps.setReplica(Boolean.TRUE);
            final Handshake handshake = new Handshake(dataMaps);
            handshake.run();
        }

        final ReadRDBFile readRDBFile = new ReadRDBFile(dataMaps);
        readRDBFile.readFromFile();
        int port = 6379;


        try {
            if (dataMaps.getConfigMap().containsKey("port"))
                port = Integer.parseInt(dataMaps.getConfigMap().get("port").trim());
        } catch (final Exception e) {

        }

        try (final ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            while (serverSocket.isBound() && !serverSocket.isClosed()) {
                final Socket clientSocket = serverSocket.accept();
                final Thread t = new Thread(new WorkerThread(clientSocket, dataMaps, replicaConnections));
                t.start();
            }
        } catch (final Exception e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }


}



