package replication;

import redisProtocol.DataMaps;
import redisProtocol.Parser;
import redisProtocol.ParserUtils;
import redisRDB.ReadRDBFile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Handshake implements Runnable {

    private DataMaps dataMaps;

    public Handshake(final DataMaps dataMaps) {
        this.dataMaps = dataMaps;
    }

    public void run() {
        try {
            final String[] addressDetail = dataMaps.getConfigMap().get("replicaof").split("\s+");
            System.out.println("address details " + addressDetail[0] + " " + addressDetail[1]);
            Socket clientSocket = new Socket(addressDetail[0], Integer.parseInt(addressDetail[1]));
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
            final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
            Parser parser = new Parser(new BufferedReader(new InputStreamReader(clientSocket.getInputStream())));
            out.write("*1\r\n$4\r\nPING\r\n");
            out.flush();
            if ("PONG".equalsIgnoreCase((String) parser.help().get(0))) {
                System.out.println("recieved pong");
                out.write("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + dataMaps.getConfigMap().get("port").length() + "\r\n" + dataMaps.getConfigMap().get("port") + "\r\n");
                out.flush();
            } else throw new RuntimeException();
            if ("OK".equalsIgnoreCase((String) parser.help().get(0))) {
                out.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
                out.flush();
            } else throw new RuntimeException();
            if ("OK".equalsIgnoreCase((String) parser.help().get(0))) {
                out.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
                out.flush();
                ReadRDBFile readRDBFile = new ReadRDBFile(dataMaps);
                readRDBFile.reader(clientSocket.getInputStream());
            } else throw new RuntimeException();

            List<Object> commands = new ArrayList<>();
            while (true) {
                try {
                    System.out.println("listening to master.....");
                    commands = parser.help();
                    ParserUtils.processLastCommand(commands, writer, dataMaps, clientSocket, true);
                    dataMaps.increaseOffset(commands.getLast().toString().getBytes(StandardCharsets.UTF_8).length);
                    if (Objects.nonNull(commands)) commands.clear();
                } catch (final Exception e) {
                    if (Objects.nonNull(commands)) commands.clear();
                }
            }
        } catch (final Exception e) {

        }

    }
}
