package replication;

import redisProtocol.DataMaps;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class Handshake {

    private DataMaps dataMaps;

    public Handshake(final DataMaps dataMaps) {
        this.dataMaps = dataMaps;
    }

    public void connect() throws IOException {
        final String[] addressDetail = dataMaps.getConfigMap().get("replicaof").split("\\s+");
        Socket clientSocket = new Socket(addressDetail[0], Integer.parseInt(addressDetail[1]));
        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        out.write("*1\r\n$4\r\nPING\r\n");
        out.flush();

    }
}
