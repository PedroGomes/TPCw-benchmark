/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package benchmarks.TpcwBenchmark;

import benchmarks.TpcwBenchmark.TPCWBenchmarkInterface.BuyingResult;
import benchmarks.dataStatistics.ResultHandler;
import benchmarks.interfaces.BenchmarkExecuter;
import benchmarks.interfaces.BenchmarkInterfaceFactory;
import benchmarks.interfaces.CRUD;

import java.util.ArrayList;
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

   
    CRUD databaseCrudFactory;
    TPCWBenchmarkInterface tpcwInterfaceFactory;
    int thread_number;
    int iterations = 1;
    int delay_time = 1000;
    String result_path;
    //  CopyOnWriteArrayList<String> costumers;
    CopyOnWriteArrayList<String> items;
    private CountDownLatch barrier;
    ResultHandler result_handler;

    public Executer(CRUD databaseCrudInterface, BenchmarkInterfaceFactory tpcwInterface, int clients, Map<String, String> info) {

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

        if (delay_info  != null) {
            delay_time = Integer.valueOf(delay_info .trim());
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

    public void execute(Map<String, Object> data) {


        items = new CopyOnWriteArrayList<String>((ArrayList<String>) data.get("ITEMS"));
        //items = (CopyOnWriteArrayList<String>) data.get("ITEMS");
        //   costumers = (CopyOnWriteArrayList<String>) data.get("COSTUMERS");

        Client[] clients =  new Client[thread_number];
        barrier = new CountDownLatch(thread_number);
        int cart_id = 0;
        for (int i = 0; i < thread_number; i++) {
            Client c = new Client(cart_id);
            clients[i] =c;
            Thread t = new Thread(c);
            t.start();
            cart_id += iterations;
        }
        try {
            barrier.await();
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        for(Client c : clients){
            result_handler.addResults(c.partial_result_handler);
        }
        result_handler.listDataToSOutput();
        result_handler.listDatatoFiles(result_path,"",true);

    }

    class Client implements Runnable {

        ResultHandler partial_result_handler;
        CRUD.CRUDclient databaseCrudclient;
        TPCWBenchmarkInterface.TPCWBenchmarkInterfaceClient tpcwInterface;
        int shoppingCart;
        Random rand;

        public Client(int shoppingCart) {
            partial_result_handler =  new ResultHandler("TPCW BENCHMARK",500);
            databaseCrudclient = databaseCrudFactory.getClient();
            tpcwInterface = (TPCWBenchmarkInterface.TPCWBenchmarkInterfaceClient) databaseCrudclient;
            this.shoppingCart = shoppingCart;
            this.rand = new Random();
        }

        public void run() {
            System.out.println("Starting client n¼"+(shoppingCart/iterations)+" : "+ iterations +"runs ");
            for (int i = 0; i < iterations; i++) {
                int waiting_time = rand.nextInt(delay_time);
                try {
                    Thread.sleep(waiting_time);
                } catch (InterruptedException ex) {
                    Logger.getLogger(Executer.class.getName()).log(Level.SEVERE, null, ex);
                }
                doRun();
                shoppingCart++;
            }

            barrier.countDown();
        }

        public void doRun() {
            int num_items = rand.nextInt(10);

            for (int i = 0; i < num_items; i++) {

                int waiting_time = rand.nextInt(delay_time);

                try {
                    Thread.sleep(waiting_time);
                } catch (InterruptedException ex) {
                    Logger.getLogger(Executer.class.getName()).log(Level.SEVERE, null, ex);
                }

                String item_id = items.get(rand.nextInt(items.size()));
                int stock = fetchItemInfo(item_id);

                int buy_X_items = 1;
                if (stock > 1) {
                    int half_items = ((stock / 2) > 5) ? 5 : (stock / 2);
                    buy_X_items = rand.nextInt(half_items) + 1;
                }
                addToCart(shoppingCart, item_id, buy_X_items);
                waiting_time = rand.nextInt(delay_time);
                try {
                    Thread.sleep(waiting_time);
                } catch (InterruptedException ex) {
                    Logger.getLogger(Executer.class.getName()).log(Level.SEVERE, null, ex);
                }
                buyCart(shoppingCart);

            }
        }

        public void buyCart(int cart_id) {
            Map<String, Integer> cart = fetchCart(cart_id);
            for (String item : cart.keySet()) {
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
}
