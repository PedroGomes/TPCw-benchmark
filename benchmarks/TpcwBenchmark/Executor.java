/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package benchmarks.TpcwBenchmark;

import benchmarks.TpcwBenchmark.TPCWBenchmarkInterface.BuyingResult;
import benchmarks.dataStatistics.ResultHandler;
import benchmarks.helpers.BenchmarkUtil;
import benchmarks.helpers.ProgressBar;
import benchmarks.interfaces.BenchmarkExecutor;
import benchmarks.interfaces.DatabaseBenchmarkInterfaceFactory;
import benchmarks.interfaces.DataBaseCRUDInterface;
import benchmarks.interfaces.ProbabilityDistribution;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author pedro
 */
public class Executor implements BenchmarkExecutor {

    private static volatile boolean aborted = false;
    boolean executionTerminated = false;
    String PersonalClientID = "";
    DataBaseCRUDInterface databaseCrudFactory;
    TPCWBenchmarkInterface tpcwInterfaceFactory;
    ProbabilityDistribution probability_distribution;
    private Map<String, Integer> TotalStock = new TreeMap<String, Integer>();
    private Map<String, Integer> BoughtItems = new TreeMap<String, Integer>();

    int thread_number;
    long networkDelay = 0;
    int iterations = 1;
    int delay_time = 1000;

    ProgressBar progressBar;
    String result_path;
    //  CopyOnWriteArrayList<String> costumers;
    CopyOnWriteArrayList<String> items;

    private CountDownLatch barrier;
    ResultHandler result_handler;

    public void init(DataBaseCRUDInterface databaseCRUDInterface, DatabaseBenchmarkInterfaceFactory DatabaseBenchmarkInterface, int clients, long networkDelay, ProbabilityDistribution distribution, Map<String, String> info) {

        this.thread_number = clients;
        this.networkDelay = networkDelay;
        this.databaseCrudFactory = databaseCRUDInterface;
        this.databaseCrudFactory.simulatedDelay(networkDelay);
        this.tpcwInterfaceFactory = (TPCWBenchmarkInterface) DatabaseBenchmarkInterface;
        this.probability_distribution = distribution;


        String iterations_info = info.get("iterations");
        if (iterations_info != null) {
            iterations = Integer.valueOf(iterations_info.trim());
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


        this.result_handler = new ResultHandler("TPCW_BENCHMARK", rounds);
    }

    /**
     * ******************************
     * <p/>
     * EXECUTION STEPS
     * <p/>
     * *****************************
     */


    public boolean prepare() {
        getItemsAndStock();
        return true;//To change body of implemented methods use File | Settings | File Templates.
    }

    public void execute(String clientID) {

//        if (!slave) {
//            DataBaseCRUDInterface.CRUDclient client = databaseCrudFactory.getClient();
//            client.truncate("ShoppingCart");
//            client.closeClient();
//        }
        PersonalClientID = clientID;
        executeBenchmark();
    }

    public void consolidate() {

        if (PersonalClientID.equals("0")) { //Master

            System.out.println("[INFO:] READING RESULTS");

            Map<String, int[]> items_FinalInfo = new TreeMap<String, int[]>();


            DataBaseCRUDInterface.CRUDclient databaseCrudClient = databaseCrudFactory.getClient();
            TPCWBenchmarkInterface.TPCWBenchmarkInterfaceClient tpcwInterface = (TPCWBenchmarkInterface.TPCWBenchmarkInterfaceClient) databaseCrudClient;
            Map<String, Map<String, Map<String, Object>>> result_info = tpcwInterface.getResults();

            for (String clientID : result_info.keySet()) {  //for each client
                System.out.println("Clients:" + result_info.keySet().size());
                Map<String, Map<String, Object>> item_info = result_info.get(clientID);
                System.out.println("Items:" + item_info.keySet().size());
                for (String itemID : item_info.keySet()) {
                    Map<String, Object> item_data = item_info.get(itemID);
                    if (!items_FinalInfo.containsKey(itemID)) {

                        Object o = databaseCrudClient.read(itemID, "Items", "I_STOCK");
                        long stock = 0;
                        if (o != null) {
                            stock = (Long) o;
                        } else {
                            System.out.println("[ERROR:]ITEM CURRENT STOCK NOT FOUND");
                        }
                        items_FinalInfo.put(itemID, new int[]{0, 0, (int) stock});
                    }
                    for (String value_name : item_data.keySet()) {
                        if (value_name.equalsIgnoreCase("STOCK")) {
                            int current_s = items_FinalInfo.get(itemID)[0];
                            int read_s = (Integer) item_data.get(value_name);
                            items_FinalInfo.get(itemID)[0] = current_s + read_s;
                        } else if (value_name.equalsIgnoreCase("BOUGHT")) {
                            int current_b = items_FinalInfo.get(itemID)[1];
                            int read_b = (Integer) item_data.get(value_name);
                            items_FinalInfo.get(itemID)[1] = current_b + read_b;
                        }
                    }
                }
            }


            ((DataBaseCRUDInterface.CRUDclient) tpcwInterface).closeClient();
            ArrayList<String> dataHeader = new ArrayList<String>();
            dataHeader.add("item_index");
            dataHeader.add("stock");
            dataHeader.add("bought");
            dataHeader.add("out_of_stock");

            int index = 0;
            for (String it : items_FinalInfo.keySet()) {

                int[] item_d = items_FinalInfo.get(it);

                ArrayList<Object> data = new ArrayList<Object>();
                int stock = item_d[0] - item_d[2];
                data.add(stock);
                data.add(item_d[1]);
                int out_of_stock = (stock - item_d[1]) < 0 ? -(stock - item_d[1]) : 0;
                data.add(out_of_stock);
                result_handler.recordData("BUYING RESULTS", index + "", data);
                index++;
            }

            result_handler.setDataHeader("BUYING RESULTS", dataHeader);
        }

        result_handler.listDataToSOutput();
        result_handler.listDatatoFiles(result_path, "", true);
    }

    public void executeBenchmark() {

        System.out.println("[INFO:] PERSONAL CLIENT ID: " + PersonalClientID);

        Client[] clients = new Client[thread_number];
        Thread[] threads = new Thread[thread_number];
        barrier = new CountDownLatch(thread_number);
        int cart_id = 0;
        progressBar = new ProgressBar(thread_number, iterations);
        for (int i = 0; i < thread_number; i++) {
            Client c = new Client(cart_id);
            clients[i] = c;
            Thread t = new Thread(c);
            threads[i] = t;
            t.start();
            cart_id += iterations;
        }
        System.out.println("[INFO:] STARTING " + thread_number + " CLIENTS -> " + iterations +" ITERATIONS");
        progressBar.printProcess(500l);
        ensureEnd controller = new ensureEnd(clients, threads);
        Thread controllerThread = new Thread(controller);
        controllerThread.start();

        try {
            barrier.await();
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        System.out.println("[INFO:] ALL CLIENTS HAVE TERMINATED");
        executionTerminated = true;

        controller.killServer();
        controllerThread.interrupt();

        for (Client c : clients) {
            result_handler.addResults(c.partial_result_handler);
            Map<String, Integer> addedStock = c.partialAddedStock;
            for (String item : addedStock.keySet()) {
                int stock = addedStock.get(item);
                int tstock = TotalStock.get(item);
                TotalStock.put(item, stock + tstock);
            }
            Map<String, Integer> partialBought = c.partialBought;
            for (String item : partialBought.keySet()) {
                if (BoughtItems.containsKey(item)) {
                    int bought = partialBought.get(item);
                    int tbought = BoughtItems.get(item);
                    BoughtItems.put(item, bought + tbought);
                } else {
                    int bought = partialBought.get(item);
                    BoughtItems.put(item, bought);
                }
            }
        }

        System.out.println("[INFO:] WRITING RESULTS TO THE DATABASE");
        DataBaseCRUDInterface.CRUDclient database_client = databaseCrudFactory.getClient();
        for (String item : TotalStock.keySet()) {
            int bought = BoughtItems.containsKey(item) ? BoughtItems.get(item) : 0;
            Results result = new Results(bought, TotalStock.get(item), PersonalClientID);
            database_client.insert(item, "Results", result);
        }

        database_client.closeClient();
        System.out.println("[INFO:] EXECUTION FINISHED");
    }

    public void getItemsAndStock() {

        ArrayList<String> fields = new ArrayList<String>();
        fields.add("I_TITLE");
        fields.add("I_STOCK");
        Map<String, Map<String, Object>> items_info = databaseCrudFactory.getClient().rangeQuery("Items", fields, -1);


        System.out.println("[INFO:] ITEMS COLLECTED FROM THE DATABASE: SIZE = " + items_info.size());
        items = new CopyOnWriteArrayList<String>();
        for (String ik : items_info.keySet()) {

            ArrayList<Object> data = new ArrayList<Object>();

            items.add(ik);
            long stock = ((Long) items_info.get(ik).get("I_STOCK"));

            TotalStock.put(ik, (int) stock);

            data.add(stock);
            result_handler.recordData("DEBUG", ik, data);


        }
        probability_distribution.init(items_info.size(), null);
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
        ProbabilityDistribution distribution = probability_distribution.getNewInstance();
        int shoppingCart;
        Map<String, Integer> partialAddedStock = new TreeMap<String, Integer>();
        Map<String, Integer> partialBought = new TreeMap<String, Integer>();

        Random rand;

        public Client(int shoppingCart) {
            partial_result_handler = new ResultHandler("TPCW BENCHMARK", 500);
            databaseCrudclient = databaseCrudFactory.getClient();
            tpcwInterface = (TPCWBenchmarkInterface.TPCWBenchmarkInterfaceClient) databaseCrudclient;
            this.shoppingCart = shoppingCart;
            this.rand = new Random();
        }

        public void run() {
            int client = shoppingCart / iterations;
//            System.out.println("Starting client n:" + (shoppingCart / iterations) + " : " + iterations + "runs ");
            int last = 0;
            for (int i = 0; i < iterations; i++) {

                progressBar.increment(client);

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
                    Logger.getLogger(Executor.class.getName()).log(Level.SEVERE, null, ex);
                }
                doRun();
                shoppingCart++;
            }
            //   System.out.println("\nClient n:" + (shoppingCart / iterations) + " terminates");
            //
            //
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
                    Logger.getLogger(Executor.class.getName()).log(Level.SEVERE, null, ex);
                }

                String item_id = items.get(distribution.getNextElement());
                int stock = fetchItemInfo(item_id);
                if (stock < 0) {
                    System.out.println("[ERROR:] CANT RETRIEVE STOCK FOR ITEM :" + item_id);
                    continue;
                }

                if (stock > 0) {
                    int limit = (stock>5) ? 5 : stock;
                    int toAdd = rand.nextInt(limit) +1;              
                    addToCart(PersonalClientID + shoppingCart, item_id, toAdd);
                }
                waiting_time = rand.nextInt(delay_time);
                try {
                    Thread.sleep(waiting_time);
                } catch (InterruptedException ex) {
                    if (aborted) {
                        interrupted = true;
                        break;
                    }
                    Logger.getLogger(Executor.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            buyCart(PersonalClientID + shoppingCart);
        }

        public void interrupt() {
            interrupted = true;

        }


        public void buyCart(String cart_id) {
            Map<String, Integer> cart = fetchCart(cart_id);
            if (Thread.interrupted() || interrupted)
                return;
            for (String item : cart.keySet()) {
                if (Thread.interrupted() || interrupted)
                    return;
                BuyingResult result = buyItem(item, cart.get(item));
                partial_result_handler.countEvent("BUYING RESULTS", result.name(), 1);

                if (result.equals(BuyingResult.BOUGHT)) {
                    if (!partialBought.containsKey(item)) {
                        partialBought.put(item, cart.get(item));

                    } else {
                        int bought = partialBought.get(item);
                        partialBought.put(item, (cart.get(item) + bought));
                    }

                }
                if (result.equals(BuyingResult.NOT_AVAILABLE)) {
                    databaseCrudclient.update(item, "Items", "I_STOCK", 10L);
                    if (!partialAddedStock.containsKey(item)) {
                        partialAddedStock.put(item, 10);
                    } else {
                        int added = partialAddedStock.get(item);
                        partialAddedStock.put(item, (10 + added));
                    }
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

        public Map<String, Integer> fetchCart(String cart_id) {
            long init_time = System.currentTimeMillis();
            Map<String, Integer> cart = tpcwInterface.readCart(cart_id);
            long end_time = System.currentTimeMillis();
            partial_result_handler.logResult("FETCH CART " + cart.size(), (end_time - init_time));
            return cart;
        }

        public void addToCart(String cart, String id, int qty) {
            long init_time = System.currentTimeMillis();
            tpcwInterface.addToCart(cart, id, qty);
            long end_time = System.currentTimeMillis();
            partial_result_handler.logResult("ADD ITEM TO CART", (end_time - init_time));
        }

        public int fetchItemInfo(String item_id) {

            long init_time = System.currentTimeMillis();
            Object o = databaseCrudclient.read(item_id, "Items", "I_STOCK");
            long stock = -1;
            if (o != null) {
                stock = (Long) o;
            }
            long end_time = System.currentTimeMillis();
            partial_result_handler.logResult("READ ITEM INFO", (end_time - init_time));
            return (int) stock;
        }

        public Map<String, Integer> getPartialAddedStock() {
            return partialAddedStock;
        }

        public Map<String, Integer> getPartialBought() {
            return partialBought;
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
                        System.out.println("[INTERRUPTION:] EXECUTOR TERMINATING : PRINTING RESULTS");


                        terminated = true;
                    }
                }

                killServer();
            } catch (IOException e) {
                if (!executionTerminated)
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                else
                    System.out.println("[INFO:] SOCKET CLOSED");
            }
        }

        public void killServer() {
            try {
                s.close();
            } catch (IOException e) {
                if (!executionTerminated)
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                else
                    System.out.println("[INFO:] SOCKET CLOSED");
            }

        }
    }
}
