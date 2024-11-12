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
        final String[] addressDetail = dataMaps.getConfigMap().get("replicaof").split("\s+");
        System.out.println("address details " + addressDetail[0] + " " + addressDetail[1]);
        Socket clientSocket = new Socket(addressDetail[0], Integer.parseInt(addressDetail[1]));
        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        out.write("*1\r\n$4\r\nPING\r\n");
        out.flush();
        BufferedReader input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        if ("OK".equalsIgnoreCase(input.readLine())) {
            out.write("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + dataMaps.getConfigMap().get("port").length() + "\r\n" + dataMaps.getConfigMap().get("port") + "\r\n");
            out.flush();
        }
        if ("OK".equalsIgnoreCase(input.readLine())) {
            out.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
            out.flush();
        }
    }
}
