import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Main {
    public static void main(String[] args) {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.out.println("Logs from your program will appear here!");

        //  Uncomment this block to pass the first stage



        int port = 6379;


        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            while (true) {
                // Since the tester restarts your program quite often, setting SO_REUSEADDR
                // ensures that we don't run into 'Address already in use' errors
                // Wait for connection from client.
                Socket clientSocket  = serverSocket.accept();
                try {
                    if (clientSocket != null) {
                        clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
                        clientSocket.close();
                    }
                } catch (IOException e) {
                    System.out.println("IOException: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}



