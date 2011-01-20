/*
 * *********************************************************************
 * Copyright (c) 2010 Pedro Gomes and Universidade do Minho.
 * All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************
 */

package org.uminho.gsd.benchmarks.TPCW_CassandraOM.database_executor;


import org.uminho.gsd.benchmarks.TPCW_CassandraOM.entities.*;
import org.uminho.gsd.benchmarks.benchmark.BenchmarkNodeID;
import org.uminho.gsd.benchmarks.dataStatistics.ResultHandler;
import org.uminho.gsd.benchmarks.helpers.BenchmarkUtil;
import org.uminho.gsd.benchmarks.interfaces.Entity;
import org.uminho.gsd.benchmarks.interfaces.KeyGenerator;
import org.uminho.gsd.benchmarks.interfaces.Workload.Operation;
import org.uminho.gsd.benchmarks.interfaces.Workload.WorkloadGeneratorInterface;
import org.uminho.gsd.benchmarks.interfaces.executor.DatabaseExecutorInterface;

import javax.jdo.*;
import javax.jdo.identity.StringIdentity;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;


public class TPCWCassandraDataNucleusExecutor implements DatabaseExecutorInterface {


    private String[] ship_types = {"AIR", "UPS", "FEDEX", "SHIP", "COURIER", "MAIL"};
    private String[] status_types = {"PROCESSING", "SHIPPED", "PENDING", "DENIED"};


    private ShoppingCart executor_cart;
    //  private Object cart_identifier;

    /**
     * The number of bought items*
     */
    int bought_qty;
    /**
     * The number of bought acts*
     */
    int bought_actions;
    /**
     * The number of bougth carts*
     */
    int bought_carts;

    /**
     * The number of 0s
     */
    int zeros;

    /**
     * Think time*
     */
    private int simulatedDelay;

    int debug = 0;

    /**
     * This client result logger*
     */
    ResultHandler client_result_handler;

    KeyGenerator keyGenerator;

    PersistenceManagerFactory pmf;

    Random random;

    public TPCWCassandraDataNucleusExecutor(KeyGenerator keyGenerator) {


        pmf = JDOHelper.getPersistenceManagerFactory("datanucleus.properties");
        this.keyGenerator = keyGenerator;
        random = new Random();
    }

    public void start(WorkloadGeneratorInterface workload, BenchmarkNodeID nodeId, int operation_number, ResultHandler handler) {
        client_result_handler = handler;

        for (int operation = 0; operation < operation_number; operation++) {
            try {
                long init_time = System.currentTimeMillis();

                Operation op = workload.getNextOperation();
                execute(op);
                long end_time = System.currentTimeMillis();

                simulatedDelay = (int) ((-Math.log(random.nextDouble()) * 7) * 1000d);
                if (simulatedDelay > 70000) {
                    simulatedDelay = 70000;
                }

                Thread.sleep(simulatedDelay);


                client_result_handler.logResult(op.getOperation(), (end_time - init_time));
            } catch (NoSuchFieldException e) {
                System.out.println("[ERROR:] THIS OPERATION DOES NOT EXIST : " + e.getMessage());
            } catch (InterruptedException e) {
                System.out.println("[ERROR:] THINK TIME AFTER METHOD EXECUTION INTERRUPTED: " + e.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        client_result_handler.getResulSet().put("total_bought", bought_qty);
        client_result_handler.getResulSet().put("buying_actions", bought_actions);
        client_result_handler.getResulSet().put("bought_carts", bought_carts);
        client_result_handler.getResulSet().put("zeros", zeros);

    }

    public void execute(Operation op) throws NoSuchFieldException {
        if (op == null) {
            System.out.println("[ERROR]: NULL OPERATION");
            return;
        }

        String method_name = op.getOperation();


        if (method_name.equalsIgnoreCase("GET_STOCK_AND_PRODUCTS")) {

            Map<String, Map<String, Object>> items_info = getItemStock_andProduct();

            op.setResult(items_info);
        } else if (method_name.equalsIgnoreCase("GET_ITEM_STOCK")) {

            String item_id = (String) op.getParameters().get("ITEM_ID");
            long stock = getItemStock(item_id);
            op.setResult(stock);

        } else if (method_name.equalsIgnoreCase("GET_BENCHMARK_RESULTS")) {
            op.setResult(getResults());

        } else if (method_name.equalsIgnoreCase("OP_HOME")) {

            int costumer = (Integer) op.getParameter("COSTUMER");
            int item_id = (Integer) op.getParameter("ITEM");
            HomeOperation("0." + costumer, item_id);

        } else if (method_name.equalsIgnoreCase("OP_SHOPPING_CART")) {

            String cart = (String) op.getParameter("CART");
            int item_id = (Integer) op.getParameter("ITEM");
            boolean create = (Boolean) op.getParameter("CREATE");
            shoppingCartInteraction(item_id, create, cart);


        } else if (method_name.equalsIgnoreCase("OP_REGISTER")) {

            //    System.out.println("REGISTARTION");

            String customer = (String) op.getParameter("CUSTOMER");

            registerCostumer(customer);

        } else if (method_name.equalsIgnoreCase("OP_LOGIN")) {

            String customer = (String) op.getParameter("CUSTOMER");

            refreshSession("0." + customer);


        } else if (method_name.equalsIgnoreCase("OP_BUY_REQUEST")) {

            String id = (String) op.getParameter("CART");

            BuyRequest(id);
        } else if (method_name.equalsIgnoreCase("OP_BUY_CONFIRM")) {

            String car_id = (String) op.getParameter("CART");
            String costumer = (String) op.getParameter("CUSTOMER");


            BuyComfirm("0." + costumer, car_id);

        } else if (method_name.equalsIgnoreCase("OP_ORDER_INQUIRY")) {
            String costumer = (String) op.getParameter("CUSTOMER");

            OrderInquiry("0." + costumer);

        } else if (method_name.equalsIgnoreCase("OP_SEARCH")) {
            String term = (String) op.getParameter("TERM");
            String field = (String) op.getParameter("FIELD");

            doSearch(term, field);

        } else if (method_name.equalsIgnoreCase("OP_NEW_PRODUCTS")) {
            String field = (String) op.getParameter("FIELD");
            newProducts(field);

        } else if (method_name.equalsIgnoreCase("OP_BEST_SELLERS")) {
            String field = (String) op.getParameter("FIELD");
            BestSellers(field);

        } else if (method_name.equalsIgnoreCase("OP_ITEM_INFO")) {
            int id = (Integer) op.getParameter("ITEM");
            ItemInfo(id);

        } else if (method_name.equalsIgnoreCase("OP_ADMIN_CHANGE")) {
            int id = (Integer) op.getParameter("ITEM");
            AdminChange(id);

        } else if (method_name.equalsIgnoreCase("")) {


        } else {
            System.out.println("[WARN:]UNKNOWN REQUESTED METHOD: " + method_name);

        }
    }


    public synchronized Object insert(String key, String path, Entity value) {


        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();
        try {
            tx.begin();
            pm.makePersistent(value);
            tx.commit();

        }

        catch (Exception ec) {
            ec.printStackTrace();
        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }

        return null;
    }

    public void remove(String key, String path, String column) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void update(String key, String path, String column, Object value, String superfield) {
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();

        long stock = (Long) value;
        try {


            tx.begin();

            Item item = pm.getObjectById(Item.class, key);
            item.setI_STOCK(stock);

            tx.commit();

        } catch (Exception e) {
            System.out.println("[ERROR]: ERROR IN STOCK CRAWLER ITERATION");
        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }


    }

    public Object read(String key, String path, String column, String superfield) {
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();

        long stock = -1;
        try {


            tx.begin();

            Item item = pm.getObjectById(Item.class, key);
            stock = item.getI_STOCK();

            tx.commit();

        } catch (Exception e) {
            System.out.println("[ERROR]: ERROR IN STOCK CRAWLER ITERATION");
        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }

        if (stock < 0) {
            return null;
        } else {
            return stock;
        }

    }

    public Map<String, Map<String, Object>> rangeQuery(String table, List<String> fields, int limit) {
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();
        Collection<Object> items = null;

        try {
            tx.begin();

            Extent e = null;
            if (table.equalsIgnoreCase("Author")) {
                e = pm.getExtent(Author.class, true);
            } else if (table.equalsIgnoreCase("Customer")) {
                e = pm.getExtent(Customer.class, true);
            } else if (table.equalsIgnoreCase("item")) {
                e = pm.getExtent(Item.class, true);
            }

            Query q = pm.newQuery(e, "");
            items = (Collection) q.execute();

            tx.commit();
        }

        catch (Exception ec) {
            ec.printStackTrace();
        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }

        Map<String, Map<String, Object>> info = new TreeMap<String, Map<String, Object>>();

        if (table.equalsIgnoreCase("Author")) {

            for (Object item : items) {
                Author author = (Author) item;
                Map<String, Object> column_info = new TreeMap<String, Object>();
                column_info.put("A_LNAME", author.getA_LNAME());
                info.put(author.getA_id() + "", column_info);
            }

        } else if (table.equals("Customer")) {

            System.out.println("I AM BEING USED ????");
            for (Object item : items) {
                Customer costumer = (Customer) item;
                Map<String, Object> column_info = new TreeMap<String, Object>();
                column_info.put("C_ADDR_ID", costumer.getAddress());
                info.put(costumer.getC_id() + "", column_info);
            }

        } else if (table.equals("item")) {


            for (Object item : items) {
                Item item_info = (Item) item;
                Map<String, Object> column_info = new TreeMap<String, Object>();
                column_info.put("I_TITLE", item_info.getI_TITLE());
                info.put(item_info.getI_id() + "", column_info);
            }

        }


        return info;
    }

    public void truncate(String path) {


        path = "org.uminho.gsd.benchmarks.TPCW_CassandraOM.entities." + path;
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();
        try {
            tx.begin();
            Query q = pm.newQuery(Class.forName(path));
            long numberInstancesDeleted = q.deletePersistentAll();
            System.out.println("Deleted " + numberInstancesDeleted + " " + path);

            tx.commit();
        }

        catch (Exception ec) {
            ec.printStackTrace();
        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }
    }

    public void index(String key, String path, Object value) {
        //not used
    }

    public void index(String key, String path, String indexed_key, Map<String, Object> value) {
        //not used
    }

    public void closeClient() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public Map<String, String> getInfo() {
        TreeMap<String, String> info = new TreeMap<String, String>();
        return info;
    }


    /******************************************/
    /****  TPCW extra benchmark operations  **/
    /****************************************/


    public long getItemStock(String item_id) {
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();
        long stock = -1;
        try {

            tx.begin();
            Item item = pm.getObjectById(Item.class, item_id);
            stock = item.getI_STOCK();
            tx.commit();

        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }
        return stock;
    }

    public Map<String, Map<String, Object>> getItemStock_andProduct() {
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();
        Collection<Item> items = null;

        try {
            tx.begin();
            Extent e = pm.getExtent(Item.class, true);
            Query q = pm.newQuery(e, "");
            items = (Collection) q.execute();

            tx.commit();
        }

        catch (Exception ec) {
            ec.printStackTrace();
        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }

        Map<String, Map<String, Object>> info = new TreeMap<String, Map<String, Object>>();

        for (Item item : items) {
            Map<String, Object> column_info = new TreeMap<String, Object>();
            column_info.put("I_STOCK", item.getI_STOCK());
            info.put(item.getI_id() + "", column_info);
        }

        return info;
    }

    static Map reverseSortByValue(Map map) {
        List list = new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                return ((Comparable) ((Map.Entry) (o1)).getValue())
                        .compareTo(((Map.Entry) (o2)).getValue());
            }
        });
        Collections.reverse(list);
        Map result = new LinkedHashMap();
        for (Iterator it = list.iterator(); it.hasNext();) {
            Map.Entry entry = (Map.Entry) it.next();
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    private Object getResults() {
        return null;
    }


    /****************************/
    /****  TPCW operations  ****/
    /**************************/


    public void HomeOperation(String costumer, int item) {


        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();

        try {

            tx.begin();

            Customer cost = pm.getObjectById(Customer.class, costumer);
            cost.getC_FNAME();
            cost.getC_LNAME();

            Item item_o = pm.getObjectById(Item.class, item);
            int related1 = item_o.getI_RELATED1();
            int related2 = item_o.getI_RELATED2();
            int related3 = item_o.getI_RELATED3();
            int related4 = item_o.getI_RELATED4();
            int related5 = item_o.getI_RELATED5();

            Item item_1 = pm.getObjectById(Item.class, related1);
            Item item_2 = pm.getObjectById(Item.class, related2);
            Item item_3 = pm.getObjectById(Item.class, related3);
            Item item_4 = pm.getObjectById(Item.class, related4);
            Item item_5 = pm.getObjectById(Item.class, related5);

            tx.commit();

        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }

    }

    public void shoppingCartInteraction(int item, boolean create, String SHOPPING_ID) {


        if (create) {

            PersistenceManager pm = pmf.getPersistenceManager();
            Transaction tx = pm.currentTransaction();

            try {

                tx.begin();

                ShoppingCart cart_aux = new ShoppingCart(SHOPPING_ID);
                cart_aux.setSC_DATE(new Timestamp(new GregorianCalendar().getTimeInMillis()));
                pm.makePersistent(cart_aux);

                tx.commit();

            }
            finally {
                if (tx.isActive()) {
                    tx.rollback();
                }
                pm.close();
            }


        }


        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();


        try {

            tx.begin();

            ShoppingCart cart_aux = pm.getObjectById(ShoppingCart.class, SHOPPING_ID);
            Item item_info = pm.getObjectById(Item.class, item);

            float cost = item_info.getI_COST();
            double srp = item_info.getSrp();
            String title = item_info.getI_TITLE();
            String backing = item_info.getI_BACKING();

            boolean found = false;

            for (ShoppingCartLine cartLine : cart_aux.getCart_lines()) {
                if (cartLine.getShoppingCartLineID().equals(SHOPPING_ID + item)) {
                    cartLine.setQty(cartLine.getQty() + 1);
                    found = true;
                    break;
                }
            }

            if (!found) {

                Item item_aux = pm.getObjectById(Item.class, item);

                //    ShoppingCartLine cartLine = new ShoppingCartLine(cart_id + "." + (executor_cart.getCart_lines().size()), item, qty);
                ShoppingCartLine cartLine = new ShoppingCartLine(SHOPPING_ID, item_aux, 1);
                cartLine.setSCL_COST(cost);
                cartLine.setSCL_SRP((float) srp);
                cartLine.setSCL_BACKING(backing);
                cartLine.setSCL_TITLE(title);

                pm.makePersistent(cartLine);
                cart_aux.addCartLine(cartLine);

            }
            JDOHelper.makeDirty(cart_aux, "cart_lines");
            Object attachedCart = pm.makePersistent(cart_aux);
            tx.commit();

        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }


    }

    public void registerCostumer(String id) {

        Customer c = generateCustomer(id);


        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();

        try {

            tx.begin();

            Country co = pm.getObjectById(Country.class, BenchmarkUtil.getRandomInt(0, 91));
            Address ad = generateAddress(co);
            c.setAddress(ad);
            pm.makePersistent(c);
            tx.commit();

        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }


    }

    public Customer generateCustomer(String id) {

        String name = (BenchmarkUtil.getRandomAString(8, 15) + " " + BenchmarkUtil.getRandomAString(8, 15));
        String[] names = name.split(" ");
        Random r = new Random();
        int random_int = r.nextInt(1000);

        String key = name + "_" + random_int;

        String pass = names[0].charAt(0) + names[1].charAt(0) + "" + random_int;
        //  insert(pass, key, "Customer", "C_PASSWD", writeCon);

        String first_name = names[0];
        //  insert(first_name, key, "Customer", "C_FNAME", writeCon);

        String last_name = names[1];
        //  insert(last_name, key, "Customer", "C_LNAME", writeCon);

        int phone = r.nextInt(999999999 - 100000000) + 100000000;
        //  insert(phone, key, "Customer", "C_PHONE", writeCon);

        String email = key + "@" + BenchmarkUtil.getRandomAString(2, 9) + ".com";
        //  insert(email, key, "Customer", "C_EMAIL", writeCon);

        double discount = r.nextDouble();
        //  insert(discount, key, "Customer", "C_DISCOUNT", writeCon);

        String adress = "Street: " + (BenchmarkUtil.getRandomAString(8, 15) + " " + BenchmarkUtil.getRandomAString(8, 15)) + " number: " + r.nextInt(500);
        //  insert(adress, key, "Customer", "C_PHONE", writeCon);


        double C_BALANCE = 0.00;
        //   insert(C_BALANCE, key, "Customer", "C_BALANCE", writeCon);

        double C_YTD_PMT = (double) BenchmarkUtil.getRandomInt(0, 99999) / 100.0;
        //   insert(C_YTD_PMT, key, "Customer", "C_YTD_PMT", writeCon);

        GregorianCalendar cal = new GregorianCalendar();
        cal.add(Calendar.DAY_OF_YEAR, -1 * BenchmarkUtil.getRandomInt(1, 730));

        java.sql.Date C_SINCE = new java.sql.Date(cal.getTime().getTime());
        //  insert(C_SINCE, key, "Customer", "C_SINCE ", writeCon);

        cal.add(Calendar.DAY_OF_YEAR, BenchmarkUtil.getRandomInt(0, 60));
        if (cal.after(new GregorianCalendar())) {
            cal = new GregorianCalendar();
        }

        java.sql.Date C_LAST_LOGIN = new java.sql.Date(cal.getTime().getTime());
        //insert(C_LAST_LOGIN, key, "Customer", "C_LAST_LOGIN", writeCon);

        java.sql.Timestamp C_LOGIN = new java.sql.Timestamp(System.currentTimeMillis());
        //insert(C_LOGIN, key, "Customer", "C_LOGIN", writeCon);

        cal = new GregorianCalendar();
        cal.add(Calendar.HOUR, 2);

        java.sql.Timestamp C_EXPIRATION = new java.sql.Timestamp(cal.getTime().getTime());
        //insert(C_EXPIRATION, key, "Customer", "C_EXPIRATION", writeCon);

        cal = BenchmarkUtil.getRandomDate(1880, 2000);
        java.sql.Date C_BIRTHDATE = new java.sql.Date(cal.getTime().getTime());
        //insert(C_BIRTHDATE, key, "Customer", "C_BIRTHDATE", writeCon);

        String C_DATA = BenchmarkUtil.getRandomAString(100, 500);
        //insert(C_DATA, key, "Customer", "C_DATA", writeCon);


        Customer c = new Customer(id, key, pass, last_name, first_name, phone, email, C_SINCE, C_LAST_LOGIN, C_LOGIN, C_EXPIRATION, C_BALANCE, C_YTD_PMT, C_BIRTHDATE, C_DATA, discount, null);
        return c;

    }

    public Address generateAddress(Country co) {


        String ADDR_STREET1, ADDR_STREET2, ADDR_CITY, ADDR_STATE;
        String ADDR_ZIP;

        ADDR_STREET1 = "street" + BenchmarkUtil.getRandomAString(10, 30);
        ADDR_STREET2 = "street" + BenchmarkUtil.getRandomAString(10, 30);
        ADDR_CITY = BenchmarkUtil.getRandomAString(4, 30);
        ADDR_STATE = BenchmarkUtil.getRandomAString(2, 20);
        ADDR_ZIP = BenchmarkUtil.getRandomAString(5, 10);


//        PersistenceManager pm = pmf.getPersistenceManager();
//        Transaction tx = pm.currentTransaction();

        Address address = null;
        //If the insertion fails, it means it already exists, or something like that...
//        try {
//
//            tx.begin();

        //  Country co = pm.getObjectById(Country.class, BenchmarkUtil.getRandomInt(0, 91));
        String key = ADDR_STREET1 + ADDR_STREET2 + ADDR_CITY + ADDR_STATE + ADDR_ZIP + co.getName();

        address = new Address(key, ADDR_STREET1, ADDR_STREET2, ADDR_CITY,
                ADDR_STATE, ADDR_ZIP, co);

        address.setCountry(co);

//            pm.makePersistent(address);
//            address = pm.detachCopy(address);
//
//
//            tx.commit();
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        finally {
//            if (tx.isActive()) {
//                tx.rollback();
//            }
//            pm.close();
//        }


        return address;
    }

    public void refreshSession(String C_ID) {


        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();


        try {

            tx.begin();

            Customer customer = pm.getObjectById(Customer.class, C_ID);
            Address add = pm.getObjectById(Address.class, customer.getAddress().getAddr_id());

            customer.setLogin(new Timestamp(System.currentTimeMillis()));
            customer.setLogin(new Timestamp(System.currentTimeMillis() + 7200000));


            tx.commit();

        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }


    }

    public void BuyRequest(String shopping_id) {
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();


        try {

            tx.begin();

            ShoppingCart cart = pm.getObjectById(ShoppingCart.class, shopping_id);
            List<ShoppingCartLine> cartLines = cart.getCart_lines();

            int qty = 0;
            float cost = 0f;

            for (ShoppingCartLine cartLine : cartLines) {

                int qty_read = cartLine.getQty();
                qty += qty_read;
                cost += (qty_read * cartLine.getSCL_COST());
            }

            float SC_SUB_TOTAL = cost * (1 - 0.2f);//cheats...
            float SC_TAX = SC_SUB_TOTAL * 0.0825f;
            float SC_SHIP_COST = 3.00f + (1.00f * qty);
            float SC_TOTAL = SC_SUB_TOTAL + SC_SHIP_COST + SC_TAX;

            cart.setSC_SUB_TOTAL(SC_SUB_TOTAL);
            cart.setSC_TAX(SC_TAX);
            cart.setSC_SHIP_COST(SC_SHIP_COST);
            cart.setSC_TOTAL(SC_TOTAL);

            tx.commit();

        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }


    }

    public void BuyComfirm(String costumer_id, String cart_id) {
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();

        try {

            tx.begin();
            //   pm.getFetchPlan().setMaxFetchDepth(2);
            ShoppingCart cart = pm.getObjectById(ShoppingCart.class, cart_id);
            List<ShoppingCartLine> cartLines = cart.getCart_lines();

            float SC_SUB_TOTAL = cart.getSC_SUB_TOTAL();
            float SC_TAX = cart.getSC_TAX();
            float SC_SHIP_COST = cart.getSC_SHIP_COST();
            float SC_TOTAL = cart.getSC_TOTAL();


            pm.getFetchPlan().addGroup("all");

            javax.jdo.identity.StringIdentity identity = new StringIdentity(Customer.class, costumer_id);

            //   System.out.println("KC:" + costumer_id.getClass().getName() + " : " + costumer_id);
            Customer cust = pm.getObjectById(Customer.class, costumer_id);


            Address c_addr = cust.getAddress();
            double c_discount = cust.getC_DISCOUNT();

            Address ship_addr_id = null;

            float decision = random.nextFloat();
            if (decision < 0.2) {

                Country co = pm.getObjectById(Country.class, BenchmarkUtil.getRandomInt(0, 91));
                ship_addr_id = generateAddress(co);

            } else {
                ship_addr_id = c_addr;
            }

            String[] ids = cart_id.split("\\.");
            int thread_id = Integer.parseInt(ids[1]);
            String key = (String) keyGenerator.getNextKey(thread_id);


            String shipping = ship_types[random.nextInt(ship_types.length)];
            String status = status_types[random.nextInt(status_types.length)];
            java.sql.Date shipDate = new java.sql.Date(System.currentTimeMillis() + random.nextInt(644444400));


            Order order = new Order(key, cust, new Date(System.currentTimeMillis()), SC_SUB_TOTAL, SC_TAX, SC_TOTAL, shipping, shipDate, status, c_addr.getAddr_id(), ship_addr_id.getAddr_id());
            List<OrderLine> order_lines = new ArrayList<OrderLine>();


            int num = 0;
            for (ShoppingCartLine cartLine : cartLines) {

                Item cart_item = cartLine.getBook();
                int item_qty = cartLine.getQty();

                OrderLine orderline = new OrderLine(order.getO_ID() + "." + num, cart_item, item_qty, c_discount, BenchmarkUtil.getRandomAString(20, 100));
                pm.makePersistent(orderline);
                order_lines.add(orderline);
                num++;
            }
            order.setOrderlines(order_lines);
            pm.makePersistent(order);


            String cc_type = BenchmarkUtil.getRandomAString(10);
            long cc_number = BenchmarkUtil.getRandomNString(16);
            String cc_name = BenchmarkUtil.getRandomAString(30);
            java.sql.Date cc_expiry = new java.sql.Date(System.currentTimeMillis() + random.nextInt(644444400));


            CCXact ccXact = new CCXact(cc_type, cc_number, cc_name, cc_expiry, SC_TOTAL, shipDate, key, ship_addr_id.getCountry().getName());
            pm.makePersistent(ccXact);

            tx.commit();

        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }
    }

    public void OrderInquiry(String customer) {   //10/15

        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();

        try {
            tx.begin();

            Order last_order = null;


            Extent e = pm.getExtent(Order.class, true);
            Query q = pm.newQuery(e, "O_C_ID == \"" + customer + "\"");
            q.setRange("0,1");
            Collection c = (Collection) q.execute();
            Iterator iter = c.iterator();
            while (iter.hasNext()) {
                Object obj = iter.next();
                if (obj instanceof Order) {
                    last_order = (Order) obj;
                }

            }
            if (last_order != null) {
                Customer cart = pm.getObjectById(Customer.class, customer);
                CCXact ccXact = pm.getObjectById(CCXact.class, last_order.getO_ID());
                List<OrderLine> lines = last_order.getOrderlines();


                Address address_b = pm.getObjectById(Address.class, last_order.getBillAddress());
                Address address_s = pm.getObjectById(Address.class, last_order.getO_SHIP_ADDR());


                for (OrderLine line : lines) {
                    Item item = line.getOL_I_ID();
                }

            }
            tx.commit();
        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }

    }

    public void doSearch(String term, String field) {   //30
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();

        try {
            tx.begin();

            if (term.equalsIgnoreCase("SUBJECT")) {

                List<Item> items = new ArrayList<Item>();

                Extent e = pm.getExtent(Item.class, true);
                Query q = pm.newQuery(e, "I_SUBJECT == \"" + field + "\"");
                Object res = q.execute();
                if (res == null)
                    return;

                Collection c = (Collection) res;
                Iterator iter = c.iterator();
                while (iter.hasNext()) {
                    Object obj = iter.next();
                    if (obj instanceof Item) {
                        items.add((Item) obj);
                    }
                }
//                System.out.print("SS:");
//                for (Item item : items) {
//                    System.out.print(" A:" + item.getI_AUTHOR().getA_LNAME());
//                }
//                System.out.println();


            } else if (term.equalsIgnoreCase("AUTHOR")) {

                Author author = null;
                Extent e = pm.getExtent(Author.class, true);
                Query q = pm.newQuery(e, "A_LNAME == \"" + field + "\"");
                q.setRange("0,1");
                Object res = q.execute();
                if (res == null)
                    return;

                Collection c = (Collection) res;
                Iterator iter = c.iterator();
                while (iter.hasNext()) {
                    Object obj = iter.next();
                    if (obj instanceof Author) {
                        author = (Author) obj;
                    }
                }
                // System.out.print("AS:");


                List<Item> items = new ArrayList<Item>();
                if (author != null) {
                    Extent extent = pm.getExtent(Item.class, true);
                    Query query = pm.newQuery(extent, "I_AUTHOR == \"" + author.getA_id() + "" + "\"");
                    Object qres = query.execute();
                    if (qres == null)
                        return;

                    Collection collection = (Collection) qres;
                    Iterator iterator = collection.iterator();
                    while (iterator.hasNext()) {
                        Object obj = iterator.next();
                        if (obj instanceof Item) {
                            items.add((Item) obj);
                        }
                    }
//                    for (Item item : items) {
//                        System.out.print(" A:" + item.getI_AUTHOR().getA_LNAME());
//                    }
//                    System.out.println();

                }


            } else if (term.equalsIgnoreCase("TITLE")) {

                List<Item> items = new ArrayList<Item>();

                Extent e = pm.getExtent(Item.class, true);
                Query q = pm.newQuery(e, "I_TITLE == \"" + field + "" + "\"");
                Object res = q.execute();
                if (res == null)
                    return;

                Collection c = (Collection) res;
                Iterator iter = c.iterator();
                while (iter.hasNext()) {
                    Object obj = iter.next();
                    if (obj instanceof Item) {
                        items.add((Item) obj);
                    }
                }
//                System.out.print("TS:");
//                for (Item item : items) {
//                    System.out.print(" A:" + item.getI_AUTHOR().getA_LNAME());
//                }
//                System.out.println();
//

            } else {
                System.out.println("OPTION NOT RECOGNIZED");


            }

            tx.commit();
        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }
    }

    private void newProducts(String field) {

        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();

        try {
            tx.begin();

            TreeMap<Long, Item> items = new TreeMap<Long, Item>();

            Extent e = pm.getExtent(Item.class, true);
            Query q = pm.newQuery(e, "I_SUBJECT == \"" + field + "\"");
            Object res = q.execute();
            if (res == null)
                return;

            Collection c = (Collection) res;
            Iterator iter = c.iterator();
            while (iter.hasNext()) {
                Object obj = iter.next();
                if (obj instanceof Item) {
                    Item i = (Item) obj;
                    if (i.getPubDate().getTime() < 0) {
                        System.out.println("NEGATIVE TIME");

                    }
                    items.put(i.getPubDate().getTime(), i);
                }
            }
            int i = 0;
            for (Map.Entry<Long, Item> entry : items.entrySet()) {
                i++;
                if (i == 50)
                    break;
            }


            tx.commit();
        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }
    }

    public void BestSellers(String field) { //30
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();

        try {
            tx.begin();
            Extent e = pm.getExtent(Order.class, true);
            Query q = pm.newQuery(e, null);
            q.setRange(0, 3333);
            Object res = q.execute();
            if (res == null)
                return;

            Collection<Order> orders = (Collection) q.execute();
            Map<Integer, Integer> items = new TreeMap<Integer, Integer>();
            Map<Integer, Author> item_author = new TreeMap<Integer, Author>();

            for (Iterator<Order> orderIterator = orders.iterator(); orderIterator.hasNext();) {
                Order order = orderIterator.next();
                List<OrderLine> lines = order.getOrderlines();
                for (OrderLine line : lines) {
                    Item i = line.getOL_I_ID();
                    if (i.getI_SUBJECT().equals(field)) {
                        int i_id = i.getI_id();

                        if (items.containsKey(i_id)) {
                            int qty = items.get(i_id);
                            items.put(i_id, qty + line.getOL_QTY());
                        } else {
                            items.put(i_id, line.getOL_QTY());
                            item_author.put(i_id, i.getI_AUTHOR());
                        }
                    }
                }
            }
            Map values = reverseSortByValue(items);
            List<Integer> best = new ArrayList<Integer>();

            int num = 0;
            for (Iterator<Integer> it = values.keySet().iterator(); it.hasNext();) {
                int key = it.next();
                best.add(key);
                //  System.out.println("K:" + key);
                num++;
                if (num == 50)
                    break;
            }

            tx.commit();
        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }

    }

    public void ItemInfo(int id) {

        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();

        try {
            tx.begin();
            Item item = pm.getObjectById(Item.class, id);
            String name = item.getI_AUTHOR().getA_LNAME();
            tx.commit();
        }
        finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }
    }

    public void AdminChange(int item_id) {


        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();

        try {
            tx.begin();
            Item item = pm.getObjectById(Item.class, item_id);

            float I_COST = random.nextInt(100);
            String image = new String("img" + random.nextInt(1000) % 100 + "/image_" + random.nextInt(1000) + ".gif");
            Date new_date = new Date(System.currentTimeMillis());
            String thumb = image.replace("image", "thumb");

            item.setI_COST(I_COST);
            item.setImage(image);
            item.setPubDate(new_date);
            item.setThumbnail(thumb);

            Extent e = pm.getExtent(Order.class, true);
            Query q = pm.newQuery(e, null);
            q.setRange(0, 10000);
            Object res = q.execute();
            if (res == null) {
                System.out.println("NULL");
                return;
            }

            Collection<Order> orders = (Collection) q.execute();

            Map<Integer, Integer> items = new TreeMap<Integer, Integer>();


            for (Iterator<Order> orderIterator = orders.iterator(); orderIterator.hasNext();) {
                Order order = orderIterator.next();
                List<OrderLine> lines = order.getOrderlines();

                boolean found = false;
                TreeMap<Integer, Integer> bought_items = new TreeMap<Integer, Integer>();

                for (OrderLine line : lines) {
                    Item i = line.getOL_I_ID();
                    int i_id = i.getI_id();
                    if (i_id == item_id) {
                        found = true;
                    } else {
                        int item_qty = line.getOL_QTY();
                        bought_items.put(i_id, item_qty);
                    }
                }
                if (found = true) {
                    for (Integer i_id : bought_items.keySet()) {
                        if (items.containsKey(i_id)) {
                            int current_qty = items.get(i_id);
                            items.put(i_id, (bought_items.get(i_id) + current_qty));
                        } else {
                            items.put(i_id, bought_items.get(i_id));
                        }
                    }
                }
            }

            Map top_sellers = reverseSortByValue(items);
            List<Integer> best = new ArrayList<Integer>();
            int num = 0;
            for (Iterator<Integer> it = top_sellers.keySet().iterator(); it.hasNext();) {
                int key = it.next();
                best.add(key);
                num++;
                if (num == 5)
                    break;
            }
            //  System.out.println("RE_"+top_sellers.toString());

            if (num < 5) {
                for (int i = num; i < 5; i++) {
                    best.add(random.nextInt(990)); //the items are form 0 to 1000 right?
                }
            }

            item.setI_RELATED1(best.get(0));
            item.setI_RELATED2(best.get(1));
            item.setI_RELATED3(best.get(2));
            item.setI_RELATED4(best.get(3));
            item.setI_RELATED5(best.get(4));

            tx.commit();
        }

        finally

        {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }


    }


}