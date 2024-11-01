import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Main {
    public static void main(String[] args) {
        int port = 6379;
        try (final ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            while (serverSocket.isBound() && !serverSocket.isClosed()) {
                final Socket clientSocket = serverSocket.accept();
                try {
                    if (clientSocket != null) {
                        process(clientSocket);
                        clientSocket.close();
                    }
                } catch (final IOException e) {
                    System.out.println("IOException: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }


    private static void process(Socket clientSocket) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()))) {

            String content;
            while ((content = reader.readLine()) != null) {
                System.out.println("::" + content);
                if ("ping".equalsIgnoreCase(content)) {
                    writer.write("+PONG\r\n");
                    writer.flush();
                } else if ("eof".equalsIgnoreCase(content)) {
                    System.out.println("eof");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}



