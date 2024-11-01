import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

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
        } catch (final IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }


    private static void process(final Socket clientSocket) {
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()))) {
            writer.write("+PONG\r\n");
            writer.flush();
        } catch (final IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}



