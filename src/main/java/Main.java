import java.io.BufferedWriter;
import java.io.IOException;
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
                        System.out.println("Starting socket == " + clientSocket.hashCode());
                        BufferedWriter writer = new BufferedWriter(
                                new OutputStreamWriter(clientSocket.getOutputStream()));
                        writer.write("+PONG\r\n");
                        writer.flush();
                        writer.write("+PONG\r\n");
                        writer.flush();
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
}



