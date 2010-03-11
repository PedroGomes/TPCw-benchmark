/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package benchmarks.TpcwBenchmark;

import benchmarks.TpcwBenchmark.TPCWBenchmarkInterface.BuyingResult;
import benchmarks.dataStatistics.ResultHandler;
import benchmarks.interfaces.BenchmarkExecuter;
import benchmarks.interfaces.CRUD;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author pedro
 */
public class Executer implements BenchmarkExecuter {

    ResultHandler result_handler;
    CRUD databaseCrudclient;
    TPCWBenchmarkInterface tpcwInterface;
    int thread_number;
    int iterations = 1;
    CopyOnWriteArrayList<String> costumers;
    CopyOnWriteArrayList<String> items ;
    private CyclicBarrier barrier;

    public Executer(CRUD databaseCrudInterface, benchmarks.interfaces.BenchmarkInterface tpcwInterface, int clients, Map<String, String> info) {

        databaseCrudclient = databaseCrudInterface;
        this.tpcwInterface = (TPCWBenchmarkInterface) tpcwInterface;
        thread_number = clients;

        String interations_info = info.get("iterations");
        if (interations_info != null) {
            iterations = Integer.valueOf(interations_info.trim());
        } else {
            System.out.println("ROUND ITERATIONS NOT FOUND: 1 DEFAULT");
        }

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
        items = (CopyOnWriteArrayList<String>) data.get("ITEMS");
        costumers = (CopyOnWriteArrayList<String>) data.get("COSTUMERS");

        barrier =  new CyclicBarrier(thread_number);
        int cart_id =  0;
        for(int i =0;i<thread_number;i++){
            Client c =  new Client(cart_id);
            Thread t = new Thread(c);
            t.start();
            cart_id += iterations;
        }


    }

    class Client implements Runnable {

        int shoppingCart;
        Random rand;

        public Client(int shoppingCart) {
            this.shoppingCart = shoppingCart;
            this.rand = new Random();
        }

        public void run() {
            for (int i = 0; i < iterations; i++) {
                int waiting_time = rand.nextInt(5000);
                try {
                    Thread.sleep(waiting_time);
                } catch (InterruptedException ex) {
                    Logger.getLogger(Executer.class.getName()).log(Level.SEVERE, null, ex);
                }
                doRun();
                shoppingCart ++;
            }
            try {
                barrier.await();
            } catch (InterruptedException ex) {
                Logger.getLogger(Executer.class.getName()).log(Level.SEVERE, null, ex);
            } catch (BrokenBarrierException ex) {
                Logger.getLogger(Executer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        public void doRun() {
            int num_items = rand.nextInt(10);

            for (int i = 0; i < num_items; i++) {

                int waiting_time = rand.nextInt(5000);

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
            }
        }

        public void buyCart(int cart_id) {
            Map<String, Integer> cart = fetchCart(cart_id);
            for (String item : cart.keySet()) {
                BuyingResult result = buyItem(item, cart.get(item));
                result_handler.countEvent("BUYING RESULTS", result.name(), 1);
                if(result.equals(BuyingResult.NOT_AVAILABLE)||result.equals(BuyingResult.NOT_AVAILABLE)){
                    databaseCrudclient.update(item, "Items", "I_STOCK", 10);
                }
            }
        }

        public BuyingResult buyItem(String item_id, int qty) {
            long init_time = System.currentTimeMillis();
            BuyingResult result = tpcwInterface.BuyCartItem(item_id, qty);
            long end_time = System.currentTimeMillis();
            result_handler.logResult("BUY ITEM", (end_time - init_time));
            return result;
        }

        public Map<String, Integer> fetchCart(int cart_id) {
            long init_time = System.currentTimeMillis();
            Map<String, Integer> cart = tpcwInterface.readCart(cart_id);
            long end_time = System.currentTimeMillis();
            result_handler.logResult("FETCH CART " + cart.size(), (end_time - init_time));
            return cart;
        }

        public void addToCart(int cart, String id, int qty) {
            long init_time = System.currentTimeMillis();
            tpcwInterface.addToCart(cart, id, qty);
            long end_time = System.currentTimeMillis();
            result_handler.logResult("ADD ITEM TO CART", (end_time - init_time));
        }

        public int fetchItemInfo(String item_id) {
            
            long init_time = System.currentTimeMillis();
            int stock = (Integer) databaseCrudclient.read(item_id, "Items", "I_STOCK");
            long end_time = System.currentTimeMillis();
            result_handler.logResult("READ ITEM INFO", (end_time - init_time));
            return stock;
        }
    }
}
