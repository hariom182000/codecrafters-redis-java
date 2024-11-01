import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

public class WorkerThread implements Runnable {

    private Socket clientSocket;

    public WorkerThread(final Socket clientSocket) {
        this.clientSocket = clientSocket;
    }


    @Override
    public void run() {
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()))) {

            String content;
            while ((content = reader.readLine()) != null) {
                System.out.println("timestamp ::: " + System.currentTimeMillis());
                System.out.println("message ::" + content);
                if ("ping".equalsIgnoreCase(content)) {
                    writer.write("+PONG\r\n");
                    writer.flush();
                } else if ("eof".equalsIgnoreCase(content)) {
                    System.out.println("eof");
                }
            }
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
