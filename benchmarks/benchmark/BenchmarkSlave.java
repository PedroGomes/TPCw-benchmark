package benchmarks.benchmark;

import benchmarks.interfaces.BenchmarkExecutor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by IntelliJ IDEA.
 * User: pedro
 * Date: Mar 25, 2010
 * Time: 5:01:33 PM
 * To change this template use File | Settings | File Templates.
 */
public class BenchmarkSlave {

    private String PersonalClientID;
    private int port;
    private PrintWriter writer;
    private BufferedReader in;
    private BenchmarkExecutor executor;


    BenchmarkSlave(int port, BenchmarkExecutor executor) {
        this.port = port;
        this.executor = executor;
    }

    public void run() {
        try {
            ServerSocket ss = new ServerSocket(port);
            System.out.println("[INFO:] Slave waiting");
            Socket clientSocket = ss.accept();
            writer = new PrintWriter(clientSocket.getOutputStream(), true);
            in = new BufferedReader(
                    new InputStreamReader(
                            clientSocket.getInputStream()));
            boolean terminated = false;

            while (!terminated) {
                String message = in.readLine();

                if (message != null && message.toUpperCase().startsWith("PREPARE")) {
                    executor.prepare();
                    PersonalClientID = message.split(" ")[1];
                    System.out.println("[INFO:] PREPARED");
                    writer.write("ACK\n");
                    writer.flush();
                }
                if (message != null && message.equalsIgnoreCase("START")) {


                    Runnable run = new Runnable() {
                        public void run() {
                            executor.execute(PersonalClientID);
                            System.out.println("[INFO:]EXECUTION ENDED ON SLAVE");
                            writer.write("EXECUTED\n");
                            writer.flush();
                        }
                    };
                    Thread t = new Thread(run);
                    t.start();


                }
                if (message != null && message.equalsIgnoreCase("ACK")) {
                    executor.consolidate();
                }
                if (message != null && message.equalsIgnoreCase("KILL")) {
                    Socket s = new Socket("localhost", 64446);
                    PrintWriter writer = new PrintWriter(s.getOutputStream(), true);
                    writer.write("KILL\n");
                    writer.flush();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }


}
