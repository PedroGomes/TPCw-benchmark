/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package benchmarks.TpcwBenchmark;

import benchmarks.TpcwBenchmark.TPCWBenchmarkInterface.BuyingResult;
import benchmarks.dataStatistics.ResultHandler;
import benchmarks.interfaces.BenchmarkExecuter;
import benchmarks.interfaces.DatabaseBenchmarkInterfaceFactory;
import benchmarks.interfaces.DataBaseCRUDInterface;
import org.apache.log4j.net.SocketServer;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author pedro
 */
public class Executer implements BenchmarkExecuter {

    public static volatile boolean aborted = false;

    DataBaseCRUDInterface databaseCrudFactory;
    TPCWBenchmarkInterface tpcwInterfaceFactory;

    int thread_number;
    int iterations = 1;
    int delay_time = 1000;
    int master_port = 55155;
    String result_path;
    //  CopyOnWriteArrayList<String> costumers;
    CopyOnWriteArrayList<String> items;
    Map<String, String> slaves;
    ArrayList<SlaveHandler> slaveHandlers;
    private CountDownLatch barrier;
    ResultHandler result_handler;

    public Executer(DataBaseCRUDInterface databaseCrudInterface, DatabaseBenchmarkInterfaceFactory tpcwInterface, int clients, Map<String, String> slaves, Map<String, String> info) {

        databaseCrudFactory = databaseCrudInterface;
        this.tpcwInterfaceFactory = (TPCWBenchmarkInterface) tpcwInterface;
        thread_number = clients;

        String interations_info = info.get("iterations");
        if (interations_info != null) {
            iterations = Integer.valueOf(interations_info.trim());
        } else {
            System.out.println("ROUND ITERATIONS NOT FOUND: 1 DEFAULT");
        }

        String delay_info = info.get("delay_times");

        if (delay_info != null) {
            delay_time = Integer.valueOf(delay_info.trim());
        } else {
            System.out.println("DEFAULT DELAY ASSUMED: 1000ms");
        }
        result_path = info.get("result_path");

        String rounds_info = info.get("rounds");
        int rounds = 500;
        if (rounds_info != null) {
            rounds = Integer.valueOf(rounds_info.trim());
        } else {
            System.out.println("ROUND NUMBER NOT FOUND: 500 DEFAULT");
        }

        String master_port_info = info.get("master_port");
        if (master_port_info != null) {
            master_port = Integer.parseInt(master_port_info.trim());
        }

        this.slaves = slaves;

        this.result_handler = new ResultHandler("TPCW_BENCHMARK", rounds);


    }

    /**
     * ******************************
     * <p/>
     * MASTER & SLAVE COORDINATION
     * <p/>
     * *****************************
     */


    public void execute(boolean use_master_mode, boolean slave) {

        if (use_master_mode && !slave) {
            DataBaseCRUDInterface.CRUDclient client = databaseCrudFactory.getClient();
            client.truncate("ShoppingCart");
            client.closeClient();
        }

        slaveHandlers = new ArrayList<SlaveHandler>();

        if (!use_master_mode) { //single node run
            getItems();
            executeBenchmark();
        } else {
            if (!slave) {//master, signal slaves
                CountDownLatch countD = new CountDownLatch(slaves.size());
                getItems();
                for (String host : slaves.keySet()) {
                    SlaveHandler sh = new SlaveHandler(host, slaves.get(host), countD);
                    slaveHandlers.add(sh);
                    Thread t = new Thread(sh);
                    t.start();
                }
                try {
                    System.out.println("[INFO:] Waiting for slaves");
                    countD.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
                System.out.println("[INFO:] STARTING SLAVES");
                for (SlaveHandler sh : slaveHandlers) {
                    sh.sendMessage("START\n");
                }
                executeBenchmark();

            } else { //slave, wait for master

                MasterHandler mh = new MasterHandler(master_port);
                Thread t = new Thread(mh);
                t.start();
            }

        }


    }

    class SlaveHandler implements Runnable {

        String host;
        int port;
        PrintWriter writer;
        CountDownLatch countBarrier;
        BufferedReader in;

        SlaveHandler(String host, String port, CountDownLatch countD) {
            this.host = host;
            this.port = Integer.parseInt(port.trim());
            this.countBarrier = countD;
            try {
                Socket cs = new Socket(host, this.port);
                writer = new PrintWriter(cs.getOutputStream(), true);
                in = new BufferedReader(
                        new InputStreamReader(
                                cs.getInputStream()));
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        public void run() {
            writer.write("PREPARE\n");
            writer.flush();
            boolean terminated = false;
            while (!terminated) {
                try {
                    String message = in.readLine();
                    if (message != null && message.equalsIgnoreCase("ACK")) {
                        terminated = true;
                    }
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
            countBarrier.countDown();
        }

        public void sendMessage(String message) {
            writer.write(message + "\n");
            writer.flush();
        }

    }


    class MasterHandler implements Runnable {

        int port;
        PrintWriter writer;
        BufferedReader in;


        MasterHandler(int port) {
            this.port = port;
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

                    if (message != null && message.equalsIgnoreCase("PREPARE")) {
                        getItems();
                        System.out.println("[INFO:] PREPARED");
                        writer.write("ACK\n");
                        writer.flush();
                    }
                    if (message != null && message.equalsIgnoreCase("START")) {
                        executeBenchmark();
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

        public void sendMessage(String message) {
            writer.write(message + "\n");
            writer.flush();
        }
    }


    public void executeBenchmark() {

        Client[] clients = new Client[thread_number];
        Thread[] threads = new Thread[thread_number];
        barrier = new CountDownLatch(thread_number);
        int cart_id = 0;
        for (int i = 0; i < thread_number; i++) {
            Client c = new Client(cart_id);
            clients[i] = c;
            Thread t = new Thread(c);
            threads[i] = t;
            t.start();
            cart_id += iterations;
        }
        ensureEnd controller = new ensureEnd(clients, threads);
        Thread controllerThread = new Thread(controller);
        controllerThread.start();

        try {
            barrier.await();
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        controller.killServer();
        controllerThread.interrupt();


        for (Client c : clients) {
            result_handler.addResults(c.partial_result_handler);

        }
        result_handler.listDataToSOutput();
        result_handler.listDatatoFiles(result_path, "", true);


    }


    public void getItems() {

        List<Object> items_names = databaseCrudFactory.getClient().rangeQuery("Items", "I_TITLE", -1);
        System.out.println("ITEMS COLLECTED FROM THE DATABASE: SIZE = " + items_names.size());
        items = new CopyOnWriteArrayList<String>();
        for (Object i : items_names) {
            items.add((String) i);
        }
    }


    /**
     * **********************
     * <p/>
     * BENCHMARK CLIENT
     * <p/>
     * ************************
     */


    class Client implements Runnable {

        private boolean interrupted = false;
        ResultHandler partial_result_handler;
        DataBaseCRUDInterface.CRUDclient databaseCrudclient;
        TPCWBenchmarkInterface.TPCWBenchmarkInterfaceClient tpcwInterface;
        int shoppingCart;
        Random rand;

        public Client(int shoppingCart) {
            partial_result_handler = new ResultHandler("TPCW BENCHMARK", 500);
            databaseCrudclient = databaseCrudFactory.getClient();
            tpcwInterface = (TPCWBenchmarkInterface.TPCWBenchmarkInterfaceClient) databaseCrudclient;
            this.shoppingCart = shoppingCart;
            this.rand = new Random();
        }

        public void run() {
            System.out.println("Starting client n¼" + (shoppingCart / iterations) + " : " + iterations + "runs ");
            int last = 0;
            for (int i = 0; i < iterations; i++) {

                int perct = ((i * 100) / iterations);
                if (perct != last && (perct % 5 == 0)) {
                    System.out.print(perct + "% ");
                    last = perct;
                }
                if (Thread.interrupted() || interrupted)
                    break;
                int waiting_time = rand.nextInt(delay_time);
                try {
                    Thread.sleep(waiting_time);
                } catch (InterruptedException ex) {
                    if (aborted) {
                        interrupted = true;
                        break;
                    }
                    Logger.getLogger(Executer.class.getName()).log(Level.SEVERE, null, ex);
                }
                doRun();
                shoppingCart++;
            }
            System.out.println("\nClient n¼" + (shoppingCart / iterations) + " terminates");
            barrier.countDown();
            databaseCrudclient.closeClient();
        }

        public void doRun() {
            int num_items = rand.nextInt(10);

            for (int i = 0; i < num_items; i++) {
                if (Thread.interrupted() || interrupted)
                    break;
                int waiting_time = rand.nextInt(delay_time);

                try {
                    Thread.sleep(waiting_time);
                } catch (InterruptedException ex) {
                    if (aborted) {
                        interrupted = true;
                        break;
                    }
                    Logger.getLogger(Executer.class.getName()).log(Level.SEVERE, null, ex);
                }

                String item_id = items.get(rand.nextInt(items.size()));
                int stock = fetchItemInfo(item_id);

                int buy_X_items = 10;
//                if (stock > 1) {
//                    int half_items = ((stock / 2) > 5) ? 5 : (stock / 2);
//                    buy_X_items = rand.nextInt(half_items) + 1;
//                }
                addToCart(shoppingCart, item_id, buy_X_items);
                waiting_time = rand.nextInt(delay_time);
                try {
                    Thread.sleep(waiting_time);
                } catch (InterruptedException ex) {
                    if (aborted) {
                        interrupted = true;
                        break;
                    }
                    Logger.getLogger(Executer.class.getName()).log(Level.SEVERE, null, ex);
                }
                buyCart(shoppingCart);

            }
        }

        public void interrupt() {
            interrupted = true;

        }


        public void buyCart(int cart_id) {
            Map<String, Integer> cart = fetchCart(cart_id);
            if (Thread.interrupted() || interrupted)
                return;
            for (String item : cart.keySet()) {
                if (Thread.interrupted() || interrupted)
                    return;
                BuyingResult result = buyItem(item, cart.get(item));
                partial_result_handler.countEvent("BUYING RESULTS", result.name(), 1);
                if (result.equals(BuyingResult.NOT_AVAILABLE) || result.equals(BuyingResult.NOT_AVAILABLE)) {
                    databaseCrudclient.update(item, "Items", "I_STOCK", 10L);
                }
            }
        }

        public BuyingResult buyItem(String item_id, int qty) {
            long init_time = System.currentTimeMillis();
            BuyingResult result = tpcwInterface.BuyCartItem(item_id, qty);
            long end_time = System.currentTimeMillis();
            partial_result_handler.logResult("BUY ITEM", (end_time - init_time));
            return result;
        }

        public Map<String, Integer> fetchCart(int cart_id) {
            long init_time = System.currentTimeMillis();
            Map<String, Integer> cart = tpcwInterface.readCart(cart_id);
            long end_time = System.currentTimeMillis();
            partial_result_handler.logResult("FETCH CART " + cart.size(), (end_time - init_time));
            return cart;
        }

        public void addToCart(int cart, String id, int qty) {
            long init_time = System.currentTimeMillis();
            tpcwInterface.addToCart(cart, id, qty);
            long end_time = System.currentTimeMillis();
            partial_result_handler.logResult("ADD ITEM TO CART", (end_time - init_time));
        }

        public int fetchItemInfo(String item_id) {

            long init_time = System.currentTimeMillis();
            long stock = (Long) databaseCrudclient.read(item_id, "Items", "I_STOCK");
            long end_time = System.currentTimeMillis();
            partial_result_handler.logResult("READ ITEM INFO", (end_time - init_time));
            return (int) stock;
        }


    }


    /**
     * **************************************************
     * <p/>
     * FORCE END CLIENT (FOR DATABASE PAINFUL SLOWDOWNS)
     * <p/>
     * *************************************************
     */


    class ensureEnd implements Runnable {

        Client[] clients;
        Thread[] threads;
        ServerSocket s;

        ensureEnd(Client[] clients, Thread[] threads) {
            this.clients = clients;
            this.threads = threads;
        }

        public void run() {
            try {
                s = new ServerSocket(64446);
                Socket clientSocket = s.accept();
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(
                                clientSocket.getInputStream()));
                boolean terminated = false;

                while (!terminated) {
                    aborted = true;
                    String message = in.readLine();

                    if (message != null && message.equalsIgnoreCase("KILL")) {
                        System.out.println("ABORTING RUN : " + clients.length);
                        int length = clients.length;
                        for (int i = 0; i < length; i++) {
                            clients[i].interrupt();
                            threads[i].interrupt();
                        }
                        System.out.println("EXECUTOR TERMINATING : PRINTING RESULTS");

                        for (SlaveHandler sh : slaveHandlers) {
                            sh.sendMessage("KILL\n");
                        }
                        terminated = true;


                    }
                }

                killServer();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        public void killServer() {
            try {
                s.close();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }

        }
    }
}
