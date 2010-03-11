/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package benchmarks.TpcwBenchmark;

import JaNaG_Source.de.beimax.janag.Namegenerator;
import benchmarks.helpers.BenchmarkUtil;
import benchmarks.interfaces.CRUD;
import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;


import benchmarks.DatabaseEngineInterfaces.CassandraInterface;
import benchmarks.TpcwBenchmark.testEntities.Address;
import benchmarks.TpcwBenchmark.testEntities.Author;
import benchmarks.TpcwBenchmark.testEntities.CCXact;
import benchmarks.TpcwBenchmark.testEntities.Costumer;
import benchmarks.TpcwBenchmark.testEntities.Country;
import benchmarks.TpcwBenchmark.testEntities.Item;
import benchmarks.TpcwBenchmark.testEntities.OrderLine;
import benchmarks.TpcwBenchmark.testEntities.Orders;
import benchmarks.helpers.JsonUtil;
import benchmarks.dataStatistics.ResultHandler;
import benchmarks.interfaces.Entity;
import benchmarks.interfaces.BenchmarkPopulator;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.TreeMap;

import java.util.concurrent.CountDownLatch;

/**
 *
 * @author pedro
 */
public class Populator implements BenchmarkPopulator {

    /**Time messuerments*/
    private static boolean delay_inserts = false;
    private static int delay_time = 100;
    private static Random rand = new Random();
    private int rounds = 500;
    private ResultHandler results;
    //ATTENTION: The NUM_EBS and NUM_ITEMS variables are the only variables
    //that should be modified in order to rescale the DB.
    private static /* final */ int NUM_EBS = 1;
    private static /* final */ int NUM_ITEMS = 1000;
    private static /* final */ int NUM_CUSTOMERS = NUM_EBS * 2880;
    private static /* final */ int NUM_ADDRESSES = 2 * NUM_CUSTOMERS;
    private static /* final */ int NUM_AUTHORS = (int) (.25 * NUM_ITEMS);
    private static /* final */ int NUM_ORDERS = (int) (.9 * NUM_CUSTOMERS);
    private static /* final */ int NUM_COUNTRIES = 92; // this is constant. Never changes!
    // static Client client;
    private static CRUD databaseClient;
    ArrayList<String> authors = new ArrayList<String>();
    ArrayList<String> addresses = new ArrayList<String>();
    ArrayList<String> countries = new ArrayList<String>();
    ArrayList<String> costumers = new ArrayList<String>();
    ArrayList<String> items = new ArrayList<String>();
    // CopyOnWriteArrayList<Orders> orders = new CopyOnWriteArrayList<Orders>();
    // CopyOnWriteArrayList<OrderLine> orderLines = new CopyOnWriteArrayList<OrderLine>();
    boolean debug = false;
    private static int num_threads = 1;
    boolean error = false;
    private CountDownLatch barrier;

    //databaseClient,number_threads_populator,benchmarkPopulatorInfo
    public Populator(CRUD databaseCrudClient, int clients, Map<String, String> info) {

        databaseClient = databaseCrudClient;
        num_threads = clients;

        String do_delays = info.get("delay_inserts");
        delay_inserts = Boolean.valueOf(do_delays.trim());
        String delay_time_info = info.get("delay_time");
        delay_time = Integer.valueOf(delay_time_info.trim());
        String rounds_info = info.get("rounds");

        if (rounds_info != null) {
            rounds = Integer.valueOf(rounds_info.trim());
            System.out.println("ROUND NUMBER NOT FOUND: 500 DEFAULT");
        }
        this.results = new ResultHandler("TPCW_POPULATOR", rounds);
    }

    /**
     * @param args the command line arguments
     */
//    public static void main(String[] args) {
//
//        ResultHandler resultHandler = new ResultHandler("TPCW benchamrk", 400);
//       // BenchmarkPopulator m = new Populator(resultHandler, 10);
//        m.populate();
//
//    }
    public boolean populate() {


        if (error) {
            return false;
        } else {
            try {
                insertCountries(NUM_COUNTRIES);
                if (delay_inserts) {
                    Thread.sleep(delay_time);
                }

                insertAddresses(NUM_ADDRESSES);
                if (delay_inserts) {
                    Thread.sleep(delay_time);
                }
                insertCostumers(NUM_CUSTOMERS);
                if (delay_inserts) {
                    Thread.sleep(delay_time);
                }
                insertAuthors(NUM_AUTHORS);
                if (delay_inserts) {
                    Thread.sleep(delay_time);
                }
                insertItems(NUM_ITEMS);
                if (delay_inserts) {
                    Thread.sleep(delay_time);
                }
                insertOrder_and_CC_XACTS(NUM_ORDERS);

                System.out.println("***Finished***");

            } catch (InterruptedException ex) {
                Logger.getLogger(BenchmarkPopulator.class.getName()).log(Level.SEVERE, null, ex);
                return false;
            } catch (Exception ex) {
                Logger.getLogger(BenchmarkPopulator.class.getName()).log(Level.SEVERE, null, ex);
                return false;
            }

            results.listDataToSOutput();
            results.cleanResults();
            return true;
        }
    }

    public Map<String, Object> getUseFullData() {
        TreeMap<String, Object> data = new TreeMap<String, Object>();
        data.put("ITEMS", items);
        data.put("COSTUMERS", costumers);
        return data;
    }

    public void removeALL() {

        databaseClient.truncate("Costumer", 70000);
        databaseClient.truncate("Items", NUM_ITEMS);
        databaseClient.truncate("Orders", NUM_ORDERS);
        databaseClient.truncate("OrderLines", NUM_ORDERS * 5);
        databaseClient.truncate("Author", NUM_AUTHORS);
        databaseClient.truncate("Countries", NUM_COUNTRIES);
        databaseClient.truncate("Addresses", 180000);

    }

    public void databaseInsert(String Operation, String key, String path, Entity value,ResultHandler results) {

        long time1 = System.currentTimeMillis();
        databaseClient.insert(key, path, value);
        long time2 = System.currentTimeMillis();
        results.logResult(Operation, time2 - time1);

    }

    /************************************************************************/
    /************************************************************************/
    /************************************************************************/
    /***************
     * Authors*
     ****************/
    public void insertAuthors(int n) throws InterruptedException {
        int threads = num_threads;
        int sections = n;
        int firstSection = 0;

        if (n < num_threads) {
            threads = 1;
            firstSection = n;
        } else {
            sections = (int) Math.floor(n / num_threads);
            int rest = n - (num_threads * sections);
            firstSection = sections + rest;
        }

        barrier = new CountDownLatch(threads);

        AuthorPopulator[] partial_authors = new AuthorPopulator[threads];
        for (int i = 0; i < threads; i++) {

            AuthorPopulator populator = null;
            if (i == 0) {
                populator = new AuthorPopulator(firstSection);

            } else {
                populator = new AuthorPopulator(sections);
            }
            partial_authors[i] = populator;
            Thread t = new Thread(populator);
            t.start();
        }

        barrier.await();
        for (AuthorPopulator authorPopulator : partial_authors) {
            ArrayList<String> ids = authorPopulator.getData();
            for (String id : ids) {
                authors.add(id);
            }
            results.addResults(authorPopulator.returnResults());
        }


    }

    class AuthorPopulator implements Runnable {

        int num_authors;
        ArrayList<String> partial_authors;
        ResultHandler partial_results;

        public AuthorPopulator(int num_authors) {
            this.num_authors = num_authors;
            partial_authors = new ArrayList<String>();
            partial_results = new ResultHandler("", rounds);
        }

        public void run() {
            this.insertAuthors(num_authors);
        }

        public void insertAuthors(int n) {

            Namegenerator ng = new Namegenerator("src/JaNaG_Source/languages.txt", "src/JaNaG_Source/semantics.txt");

            System.out.println("Inserting Authors: " + n);
            for (int i = 0; i < n; i++) {
                GregorianCalendar cal = BenchmarkUtil.getRandomDate(1800, 1990);

                String[] names = (ng.getRandomName("US-Zensus", "Männlich Top 500+")).split(" ");
                String[] Mnames = (ng.getRandomName("US-Zensus", "Männlich Top 500+")).split(" ");

                String first_name = names[0];
                String last_name = names[1];
                String middle_name = Mnames[1];
                java.sql.Date dob = new java.sql.Date(cal.getTime().getTime());
                String bio = BenchmarkUtil.getRandomAString(125, 500);
                String key = first_name + middle_name + last_name + rand.nextInt(1000);

//            insert(first_name, key, "Author", "A_FNAME", writeConsistency);
//            insert(last_name, key, "Author", "A_LNAME", writeConsistency);
//            insert(middle_name, key, "Author", "A_MNAME", writeConsistency);
//            insert(dob, key, "Author", "A_DOB", writeConsistency);
//            insert(bio, key, "Author", "A_BIO", writeConsistency);

                Author a = new Author(key, first_name, last_name, middle_name, dob, bio);
                databaseInsert("INSERT Authors", key, "Author", a,partial_results);

                partial_authors.add(a.getA_id());
            }
            if (debug) {
                System.out.println("Thread finished: " + num_authors + " authors inserted");
            }

            barrier.countDown();

        }

        public ArrayList<String> getData() {
            return partial_authors;
        }

        public ResultHandler returnResults() {
            return partial_results;
        }
    }

    /***************
     * Costumers*
     ****************/
    public void insertCostumers(int n) throws InterruptedException {

        int threads = num_threads;
        int sections = n;
        int firstSection = 0;

        if (n < num_threads) {
            threads = 1;
            firstSection = n;
        } else {
            sections = (int) Math.floor(n / num_threads);
            int rest = n - (num_threads * sections);
            firstSection = sections + rest;
        }

        barrier = new CountDownLatch(threads);

        CostumerPopulator[] partial_costumers = new CostumerPopulator[threads];
        for (int i = 0; i < threads; i++) {

            CostumerPopulator populator = null;
            if (i == 0) {
                populator = new CostumerPopulator(firstSection);

            } else {
                populator = new CostumerPopulator(sections);
            }
            partial_costumers[i] = populator;
            Thread t = new Thread(populator);
            t.start();
        }
        barrier.await();
        for (CostumerPopulator costumerPopulator : partial_costumers) {
            ArrayList<String> ids = costumerPopulator.getData();
            for (String id : ids) {
                costumers.add(id);
            }
            results.addResults(costumerPopulator.returnResults());
        }


    }

    class CostumerPopulator implements Runnable {

        int num_costumers;
        ArrayList<String> partial_costumers;
        ResultHandler partial_results;

        public CostumerPopulator(int num_costumers) {
            this.num_costumers = num_costumers;
            partial_costumers = new ArrayList<String>();
            partial_results = new ResultHandler("", rounds);

        }

        public void run() {
            this.insertCostumers(num_costumers);
        }

        public void insertCostumers(int n) {

            Namegenerator ng = new Namegenerator("src/JaNaG_Source/languages.txt", "src/JaNaG_Source/semantics.txt");

            System.out.println("Inserting Costumers: " + n);
            for (int i = 0; i < n; i++) {

                String name = ng.getRandomName("US-Zensus", "Männlich Top 500+");
                String[] names = name.split(" ");
                Random r = new Random();
                int random_int = r.nextInt(1000);

                String key = name + "_" + random_int;

                String pass = names[0].charAt(0) + names[1].charAt(0) + "" + random_int;
                //  insert(pass, key, "Costumer", "C_PASSWD", writeCon);

                String first_name = names[0];
                //  insert(first_name, key, "Costumer", "C_FNAME", writeCon);

                String last_name = names[1];
                //  insert(last_name, key, "Costumer", "C_LNAME", writeCon);

                int phone = r.nextInt(999999999 - 100000000) + 100000000;
                //  insert(phone, key, "Costumer", "C_PHONE", writeCon);

                String email = key + "@email.com";
                //  insert(email, key, "Costumer", "C_EMAIL", writeCon);

                double discount = r.nextDouble();
                //  insert(discount, key, "Costumer", "C_DISCOUNT", writeCon);

                String adress = "Street: " + ng.getRandomName("US-Zensus", "Weiblich Ungewöhnlich") + " number: " + r.nextInt(500);
                //  insert(adress, key, "Costumer", "C_PHONE", writeCon);


                double C_BALANCE = 0.00;
                //   insert(C_BALANCE, key, "Costumer", "C_BALANCE", writeCon);

                double C_YTD_PMT = (double) BenchmarkUtil.getRandomInt(0, 99999) / 100.0;
                //   insert(C_YTD_PMT, key, "Costumer", "C_YTD_PMT", writeCon);

                GregorianCalendar cal = new GregorianCalendar();
                cal.add(Calendar.DAY_OF_YEAR, -1 * BenchmarkUtil.getRandomInt(1, 730));

                java.sql.Date C_SINCE = new java.sql.Date(cal.getTime().getTime());
                //  insert(C_SINCE, key, "Costumer", "C_SINCE ", writeCon);

                cal.add(Calendar.DAY_OF_YEAR, BenchmarkUtil.getRandomInt(0, 60));
                if (cal.after(new GregorianCalendar())) {
                    cal = new GregorianCalendar();
                }

                java.sql.Date C_LAST_LOGIN = new java.sql.Date(cal.getTime().getTime());
                //insert(C_LAST_LOGIN, key, "Costumer", "C_LAST_LOGIN", writeCon);

                java.sql.Timestamp C_LOGIN = new java.sql.Timestamp(System.currentTimeMillis());
                //insert(C_LOGIN, key, "Costumer", "C_LOGIN", writeCon);

                cal = new GregorianCalendar();
                cal.add(Calendar.HOUR, 2);

                java.sql.Timestamp C_EXPIRATION = new java.sql.Timestamp(cal.getTime().getTime());
                //insert(C_EXPIRATION, key, "Costumer", "C_EXPIRATION", writeCon);

                cal = BenchmarkUtil.getRandomDate(1880, 2000);
                java.sql.Date C_BIRTHDATE = new java.sql.Date(cal.getTime().getTime());
                //insert(C_BIRTHDATE, key, "Costumer", "C_BIRTHDATE", writeCon);

                String C_DATA = BenchmarkUtil.getRandomAString(100, 500);
                //insert(C_DATA, key, "Costumer", "C_DATA", writeCon);

                String address_id = addresses.get(rand.nextInt(addresses.size()));
                //insert(address.getAddr_id(), key, "Costumer", "C_ADDR_ID", writeCon);

                Costumer c = new Costumer(key, key, pass, last_name, first_name, phone, email, C_SINCE, C_LAST_LOGIN, C_LOGIN, C_EXPIRATION, C_BALANCE, C_YTD_PMT, C_BIRTHDATE, C_DATA, discount, address_id);

                databaseInsert("INSERT COSTUMERS", key, "Costumer", c,partial_results);


                partial_costumers.add(c.getC_id());


            }
            if (debug) {
                System.out.println("Thread finished: " + num_costumers + " costumers inserted");
            }
            barrier.countDown();
        }

        public ArrayList<String> getData() {
            return partial_costumers;
        }

        public ResultHandler returnResults() {
            return partial_results;
        }
    }

    /***************
     * Items*
     ****************/
    public void insertItems(int n) throws InterruptedException {
        int threads = num_threads;
        int sections = n;
        int firstSection = 0;

        if (n < num_threads) {
            threads = 1;
            firstSection = n;
        } else {
            sections = (int) Math.floor(n / num_threads);
            int rest = n - (num_threads * sections);
            firstSection = sections + rest;
        }

        barrier = new CountDownLatch(threads);

        ItemPopulator[] partial_items = new ItemPopulator[threads];
        for (int i = 0; i < threads; i++) {

            ItemPopulator populator = null;
            if (i == 0) {
                populator = new ItemPopulator(firstSection);

            } else {
                populator = new ItemPopulator(sections);
            }
            partial_items[i] = populator;
            Thread t = new Thread(populator);
            t.start();
        }
        barrier.await();

        for (ItemPopulator itemPopulator : partial_items) {
            ArrayList<String> ids = itemPopulator.getData();
            for (String id : ids) {
                items.add(id);
            }
            results.addResults(itemPopulator.returnResults());



        }

    }

    class ItemPopulator implements Runnable {

        int num_items;
        ArrayList<String> partial_items;
        ResultHandler partial_results;

        public ItemPopulator(int num_items) {
            this.num_items = num_items;
            partial_items = new ArrayList<String>();
            partial_results = new ResultHandler("", rounds);
        }

        public void run() {
            this.insertItems(num_items);
        }

        public void insertItems(int n) {

            String[] subjects = {"ARTS", "BIOGRAPHIES", "BUSINESS", "CHILDREN",
                "COMPUTERS", "COOKING", "HEALTH", "HISTORY",
                "HOME", "HUMOR", "LITERATURE", "MYSTERY",
                "NON-FICTION", "PARENTING", "POLITICS",
                "REFERENCE", "RELIGION", "ROMANCE",
                "SELF-HELP", "SCIENCE-NATURE", "SCIENCE-FICTION",
                "SPORTS", "YOUTH", "TRAVEL"};
            String[] backings = {"HARDBACK", "PAPERBACK", "USED", "AUDIO",
                "LIMITED-EDITION"};


            Namegenerator ng = new Namegenerator("src/JaNaG_Source/languages.txt", "src/JaNaG_Source/semantics.txt");


            String column_family = "Items";

            System.out.println("Inserting Items: " + n);

            ArrayList<String> titles = new ArrayList<String>();
            for (int i = 0; i < n; i++) {
                boolean rad1 = rand.nextBoolean();
                String f_name = rad1 ? ng.getRandomName("Menschlich Fantasy", "Mann") : ng.getRandomName("Menschlich Fantasy", "Frau");
                String l_name = rad1 ? ng.getRandomName("Hebräisch", "Mann") : ng.getRandomName("Hebräisch", "Frau");
                int num = rand.nextInt(1000);
                titles.add(f_name + " and " + l_name + " " + num);
            }

            for (int i = 0; i < n; i++) {


                String I_TITLE; //ID
                String I_AUTHOR;
                String I_PUBLISHER;
                String I_DESC;
                String I_SUBJECT;
                float I_COST;
                long I_STOCK;
                List<String> I_RELATED = new ArrayList<String>();
                int I_PAGE;
                String I_BACKING;
                I_TITLE = titles.get(i);


                int author_pos = rand.nextInt(authors.size());
                String author = authors.get(author_pos);

                boolean rad1 = rand.nextBoolean();
                I_AUTHOR = rad1 ? ng.getRandomName("Götternamen", "Gott") : ng.getRandomName("Götternamen", "Göttin");
                //     insert(I_AUTHOR, I_TITLE, column_family, "I_AUTHOR", writeCon);


                rad1 = rand.nextBoolean();
                I_PUBLISHER = rad1 ? ng.getRandomName("Polnisch", "Mann") : ng.getRandomName("Polnisch", "Frau");
                //    insert(I_PUBLISHER, I_TITLE, column_family, "I_PUBLISHER", writeCon);

                rad1 = rand.nextBoolean();
                I_DESC = null;
                if (rad1) {
                    boolean rad2 = rand.nextBoolean();
                    I_DESC = rad2 ? ng.getRandomName("Spanisch", "Mann") : ng.getRandomName("Spanisch", "Frau");
                    //      insert(I_DESC, I_TITLE, column_family, "I_DESC", writeCon);
                }

                I_COST = rand.nextInt(100);
                // insert(I_AUTHOR, I_TITLE, column_family, "I_AUTHOR", writeCon);

                I_STOCK = rand.nextInt(10);
                // insert(I_STOCK, I_TITLE, column_family, "I_STOCK", writeCon);

                int related_number = rand.nextInt(5);
                for (int z = 0; z < related_number; z++) {
                    String title = titles.get(rand.nextInt(n));
                    if (!I_RELATED.contains(title)) {
                        I_RELATED.add(title);
                    }
                }
                if (related_number > 0) {
                    //   insert(I_RELATED, I_TITLE, column_family, "I_RELATED", writeCon);
                }

                I_PAGE = rand.nextInt(500) + 10;
                //  insert(I_PAGE, I_TITLE, column_family, "I_PAGE", writeCon);

                I_SUBJECT = subjects[rand.nextInt(subjects.length - 1)];
                // insert(I_SUBJECT, I_TITLE, column_family, "I_SUBJECT", writeCon);

                I_BACKING = backings[rand.nextInt(backings.length - 1)];
                //insert(I_BACKING, I_TITLE, column_family, "I_BACKING", writeCon);


                GregorianCalendar cal = BenchmarkUtil.getRandomDate(1930, 2000);

                java.sql.Date pubDate = new java.sql.Date(cal.getTime().getTime());



                String thumbnail = new String("img" + i % 100 + "/thumb_" + i + ".gif");
                String image = new String("img" + i % 100 + "/image_" + i + ".gif");

                double srp = (double) BenchmarkUtil.getRandomInt(100, 99999);
                srp /= 100.0;

                String isbn = BenchmarkUtil.getRandomAString(13);

                java.sql.Date avail = new java.sql.Date(cal.getTime().getTime()); //Data when available

                String dimensions = ((double) BenchmarkUtil.getRandomInt(1, 9999) / 100.0) + "x"
                        + ((double) BenchmarkUtil.getRandomInt(1, 9999) / 100.0) + "x"
                        + ((double) BenchmarkUtil.getRandomInt(1, 9999) / 100.0);



                Item item = new Item(I_TITLE, I_TITLE, pubDate, I_PUBLISHER, I_DESC, I_SUBJECT, thumbnail, image, I_COST, I_STOCK, isbn, srp, I_RELATED, I_PAGE, avail, I_BACKING, dimensions, author);

                databaseInsert("INSERT ITEMS", I_TITLE, column_family, item,partial_results);

                partial_items.add(item.getI_id());

            }
            if (debug) {
                System.out.println("Thread finished: " + num_items + " items inserted");
            }

            barrier.countDown();
        }

        public ArrayList<String> getData() {
            return partial_items;
        }

        public ResultHandler returnResults() {
            return partial_results;
        }
    }

    /***********
     * Addresses*
     ***********/
    public void insertAddresses(int n) throws InterruptedException {

        int threads = num_threads;
        int sections = n;
        int firstSection = 0;

        if (n < num_threads) {
            threads = 1;
            firstSection = n;
        } else {
            sections = (int) Math.floor(n / num_threads);
            int rest = n - (num_threads * sections);
            firstSection = sections + rest;
        }

        barrier = new CountDownLatch(threads);
        AddressPopulator[] partial_addresses = new AddressPopulator[threads];
        for (int i = 0; i < threads; i++) {

            AddressPopulator populator = null;
            if (i == 0) {
                populator = new AddressPopulator(firstSection);

            } else {
                populator = new AddressPopulator(sections);
            }
            Thread t = new Thread(populator);
            partial_addresses[i] = populator;
            t.start();
        }
        barrier.await();

        for (AddressPopulator addressPopulator : partial_addresses) {

              ArrayList<String> ids =  addressPopulator.getData();
              for(String id : ids){
                addresses.add(id);
              }
              results.addResults(addressPopulator.returnResults());
        }

    }

    class AddressPopulator implements Runnable {

        int num_addresses;
        ArrayList<String> partial_adresses;
        ResultHandler partial_results;

        public AddressPopulator(int num_addresses) {
            this.num_addresses = num_addresses;
            partial_adresses = new ArrayList<String>();
            partial_results = new ResultHandler("", rounds);
        }

        public void run() {
            this.insertAddress(num_addresses);
        }

        private void insertAddress(int n) {

            System.out.println("Inserting Address: " + n);

            String ADDR_STREET1, ADDR_STREET2, ADDR_CITY, ADDR_STATE;
            String ADDR_ZIP;
            String country_id;

            for (int i = 0; i < n; i++) {
                ADDR_STREET1 = "street" + BenchmarkUtil.getRandomAString(10, 30);


                ADDR_STREET2 = "street" + BenchmarkUtil.getRandomAString(10, 30);
                ADDR_CITY = BenchmarkUtil.getRandomAString(4, 30);
                ADDR_STATE = BenchmarkUtil.getRandomAString(2, 20);
                ADDR_ZIP = BenchmarkUtil.getRandomAString(5, 10);
                country_id = countries.get(BenchmarkUtil.getRandomInt(0, NUM_COUNTRIES - 1));


                String key = country_id + ADDR_STATE + ADDR_CITY + ADDR_ZIP + rand.nextInt(1000);

                Address address = new Address(key, ADDR_STREET1, ADDR_STREET2, ADDR_CITY,
                        ADDR_STATE, ADDR_ZIP, country_id);
//            insert(ADDR_STREET1, key, "Addresses", "ADDR_STREET1", writeConsistency);
//            insert(ADDR_STREET2, key, "Addresses", "ADDR_STREET2", writeConsistency);
//            insert(ADDR_STATE, key, "Addresses", "ADDR_STATE", writeConsistency);
//            insert(ADDR_CITY, key, "Addresses", "ADDR_CITY", writeConsistency);
//            insert(ADDR_ZIP, key, "Addresses", "ADDR_ZIP", writeConsistency);
//            insert(country.getCo_id(), key, "Addresses", "ADDR_CO_ID", writeConsistency);

                databaseInsert("INSERT Adresses", key, "Addresses", address,partial_results);
                partial_adresses.add(address.getAddr_id());


            }
            if (debug) {
                System.out.println("Thread finished: " + num_addresses + " addresses.");
            }

            barrier.countDown();
        }

        public ArrayList<String> getData() {
            return partial_adresses;
        }

        public ResultHandler returnResults() {
            return partial_results;
        }
    }

    /***********
     *Countries *
     ***********/
    private void insertCountries(int numCountries) {

        String[] countriesNames = {
            "United States", "United Kingdom", "Canada", "Germany", "France", "Japan",
            "Netherlands", "Italy", "Switzerland", "Australia", "Algeria", "Argentina",
            "Armenia", "Austria", "Azerbaijan", "Bahamas", "Bahrain", "Bangla Desh",
            "Barbados", "Belarus", "Belgium", "Bermuda", "Bolivia", "Botswana", "Brazil",
            "Bulgaria", "Cayman Islands", "Chad", "Chile", "China", "Christmas Island",
            "Colombia", "Croatia", "Cuba", "Cyprus", "Czech Republic", "Denmark",
            "Dominican Republic", "Eastern Caribbean", "Ecuador", "Egypt", "El Salvador",
            "Estonia", "Ethiopia", "Falkland Island", "Faroe Island", "Fiji", "Finland",
            "Gabon", "Gibraltar", "Greece", "Guam", "Hong Kong", "Hungary", "Iceland",
            "India", "Indonesia", "Iran", "Iraq", "Ireland", "Israel", "Jamaica", "Jordan",
            "Kazakhstan", "Kuwait", "Lebanon", "Luxembourg", "Malaysia", "Mexico",
            "Mauritius", "New Zealand", "Norway", "Pakistan", "Philippines", "Poland",
            "Portugal", "Romania", "Russia", "Saudi Arabia", "Singapore", "Slovakia",
            "South Africa", "South Korea", "Spain", "Sudan", "Sweden", "Taiwan",
            "Thailand", "Trinidad", "Turkey", "Venezuela", "Zambia"
        };

        double[] exchanges = {
            1, .625461, 1.46712, 1.86125, 6.24238, 121.907, 2.09715, 1842.64, 1.51645,
            1.54208, 65.3851, 0.998, 540.92, 13.0949, 3977, 1, .3757, 48.65, 2, 248000,
            38.3892, 1, 5.74, 4.7304, 1.71, 1846, .8282, 627.1999, 494.2, 8.278,
            1.5391, 1677, 7.3044, 23, .543, 36.0127, 7.0707, 15.8, 2.7, 9600, 3.33771,
            8.7, 14.9912, 7.7, .6255, 7.124, 1.9724, 5.65822, 627.1999, .6255, 309.214,
            1, 7.75473, 237.23, 74.147, 42.75, 8100, 3000, .3083, .749481, 4.12, 37.4,
            0.708, 150, .3062, 1502, 38.3892, 3.8, 9.6287, 25.245, 1.87539, 7.83101, 52,
            37.8501, 3.9525, 190.788, 15180.2, 24.43, 3.7501, 1.72929, 43.9642, 6.25845,
            1190.15, 158.34, 5.282, 8.54477, 32.77, 37.1414, 6.1764, 401500, 596, 2447.7
        };

        String[] currencies = {
            "Dollars", "Pounds", "Dollars", "Deutsche Marks", "Francs", "Yen", "Guilders",
            "Lira", "Francs", "Dollars", "Dinars", "Pesos", "Dram", "Schillings", "Manat",
            "Dollars", "Dinar", "Taka", "Dollars", "Rouble", "Francs", "Dollars",
            "Boliviano", "Pula", "Real", "Lev", "Dollars", "Franc", "Pesos", "Yuan Renmimbi",
            "Dollars", "Pesos", "Kuna", "Pesos", "Pounds", "Koruna", "Kroner", "Pesos",
            "Dollars", "Sucre", "Pounds", "Colon", "Kroon", "Birr", "Pound", "Krone", "Dollars",
            "Markka", "Franc", "Pound", "Drachmas", "Dollars", "Dollars", "Forint", "Krona",
            "Rupees", "Rupiah", "Rial", "Dinar", "Punt", "Shekels", "Dollars", "Dinar", "Tenge",
            "Dinar", "Pounds", "Francs", "Ringgit", "Pesos", "Rupees", "Dollars", "Kroner",
            "Rupees", "Pesos", "Zloty", "Escudo", "Leu", "Rubles", "Riyal", "Dollars", "Koruna",
            "Rand", "Won", "Pesetas", "Dinar", "Krona", "Dollars", "Baht", "Dollars", "Lira",
            "Bolivar", "Kwacha"
        };

        if (numCountries > countriesNames.length) {
            numCountries = countriesNames.length - 1;
        }


        System.out.println("Inserting Countries: " + numCountries);

        for (int i = 0; i < numCountries; i++) {

            //Country name = key
            //insert(exchanges[i], countriesNames[i], "Countries", "CO_EXCHANGE", writeConsitency);
            //insert(currencies[i], countriesNames[i], "Countries", "CO_CURRENCY", writeConsitency);
            Country country = new Country(countriesNames[i], countriesNames[i], exchanges[i], currencies[i]);
            databaseInsert("INSERT Countries", countriesNames[i], "Countries", country,results);
            this.countries.add(country.getCo_id());
        }
        if (debug) {
            System.out.println("Countries:" + countriesNames.length + " inserted");
        }
    }

    /*******************
     * Orders and XACTS *
     ********************/
    public void insertOrder_and_CC_XACTS(int n) throws InterruptedException {

        int threads = num_threads;
        int sections = n;
        int firstSection = 0;

        if (n < num_threads) {
            threads = 1;
            firstSection = n;
        } else {
            sections = (int) Math.floor(n / num_threads);
            int rest = n - (num_threads * sections);
            firstSection = sections + rest;
        }

        barrier = new CountDownLatch(threads);

        int lastkey = 1;
        Order_and_XACTSPopulator[] partial_orders = new Order_and_XACTSPopulator[threads];
        for (int i = 0; i < threads; i++) {

            Order_and_XACTSPopulator populator = null;
            if (i == 0) {
                populator = new Order_and_XACTSPopulator(lastkey, firstSection);
                lastkey = lastkey + firstSection;

            } else {
                populator = new Order_and_XACTSPopulator(lastkey, sections);
                lastkey = lastkey + sections;
            }
            partial_orders[i] = populator;
            Thread t = new Thread(populator);
            t.start();
        }
        barrier.await();

        for (Order_and_XACTSPopulator orderPopulator : partial_orders) {
            results.addResults(orderPopulator.returnResults());
        }


    }

    class Order_and_XACTSPopulator implements Runnable {

        int num_orders;
        int begin_key;
        ResultHandler partial_results;

        public Order_and_XACTSPopulator(int num_orders, int begin_key) {
            this.num_orders = num_orders;
            this.begin_key = begin_key;
            partial_results = new ResultHandler("", rounds);
        }

        public void run() {
            this.insertOrder_and_CC_XACTS(begin_key, num_orders);
        }

        public void insertOrder_and_CC_XACTS(int begin_key, int number_keys) {


            System.out.println("Inserting Orders: " + number_keys);

            String column_family = "Orders";
            String subcolumn_family = "OrderLines";
            Namegenerator ng = new Namegenerator("src/JaNaG_Source/languages.txt", "src/JaNaG_Source/semantics.txt");
            String[] credit_cards = {"VISA", "MASTERCARD", "DISCOVER", "AMEX", "DINERS"};
            String[] ship_types = {"AIR", "UPS", "FEDEX", "SHIP", "COURIER", "MAIL"};
            String[] status_types = {"PROCESSING", "SHIPPED", "PENDING", "DENIED"};

            long O_ID = begin_key;
            // ColumnPath path = new ColumnPath(column_family);
            // path.setSuper_column("ids".getBytes());

            for (int z = 0; z < number_keys; z++) {

                column_family = "Orders";
                subcolumn_family = "OrderLines";

                String O_C_ID;
                java.sql.Timestamp O_DATE;
                float O_SUB_TOTAL;
                float O_TAX;
                float O_TOTAL;
                java.sql.Timestamp O_SHIP_DATE;
                String O_SHIP_TYPE;
                String O_SHIP_ADDR;
                String O_STATUS;




                String costumer_id = costumers.get(rand.nextInt(costumers.size()));

                O_C_ID = costumer_id;

                GregorianCalendar call = new GregorianCalendar();
                O_DATE = new java.sql.Timestamp(call.getTime().getTime());
                //insertInSuperColumn(O_DATE, O_C_ID, column_family, O_ID + "", "O_DATE", write_con);

                O_SUB_TOTAL = rand.nextFloat() * 100 * 4;
                //insertInSuperColumn(O_SUB_TOTAL, O_C_ID, column_family, O_ID + "", "O_SUB_TOTAL", write_con);

                O_TAX = O_SUB_TOTAL * 0.21f;
                //insertInSuperColumn(O_TAX, O_C_ID, column_family, O_ID + "", "O_TAX", write_con);

                O_TOTAL = O_SUB_TOTAL + O_TAX;
                //insertInSuperColumn(O_TOTAL, O_C_ID, column_family, O_ID + "", "O_TOTAL", write_con);

                call.add(Calendar.DAY_OF_YEAR, BenchmarkUtil.getRandomInt(0, 7));
                O_SHIP_DATE = new java.sql.Timestamp(call.getTime().getTime());
                //insertInSuperColumn(O_SHIP_DATE, O_C_ID, column_family, O_ID + "", "O_SHIP_DATE", write_con);

                O_SHIP_TYPE = ship_types[rand.nextInt(ship_types.length)];
                //insertInSuperColumn(O_SHIP_TYPE, O_C_ID, column_family, O_ID + "", "O_SHIP_TYPE", write_con);

                O_STATUS = status_types[rand.nextInt(status_types.length)];
                //insertInSuperColumn(O_STATUS, O_C_ID, column_family, O_ID + "", "O_STATUS", write_con);

                String billAddress = addresses.get(BenchmarkUtil.getRandomInt(0, NUM_ADDRESSES));
                // insertInSuperColumn(billAddress.getAddr_id(), O_C_ID, column_family, O_ID + "", "O_BILL_ADDR_ID", write_con);

                O_SHIP_ADDR = addresses.get(BenchmarkUtil.getRandomInt(0, NUM_ADDRESSES));
                // insertInSuperColumn(O_SHIP_ADDR.getAddr_id(), O_C_ID, column_family, O_ID + "", "O_SHIP_ADDR_ID", write_con);

                Orders order = new Orders(O_ID, O_C_ID, O_DATE, O_SUB_TOTAL, O_TAX, O_TOTAL, O_SHIP_TYPE, O_SHIP_DATE, O_STATUS, billAddress, O_SHIP_ADDR);

                databaseInsert("INSERT Orders", O_C_ID, column_family, order,partial_results);
                //orders.add(order);


                int number_of_items = rand.nextInt(4) + 1;

                for (int i = 0; i < number_of_items; i++) {
                    /**
                     * OL_ID
                     * OL_O_ID
                     * OL_I_ID
                     * OL_QTY
                     * OL_DISCOUNT
                     * OL_COMMENT
                     */
                    String OL_ID;

                    String OL_I_ID;
                    int OL_QTY;
                    float OL_DISCOUNT;
                    String OL_COMMENT;

                    OL_ID = O_ID + "N" + i;


                    OL_I_ID = items.get(rand.nextInt(items.size()));
                    //insertInSuperColumn(OL_I_ID, O_ID + "", subcolumn_family, OL_ID, "OL_I_ID", write_con);

                    OL_QTY = rand.nextInt(4) + 1;
                    //insertInSuperColumn(OL_QTY, O_ID + "", subcolumn_family, OL_ID, "OL_QTY", write_con);

                    OL_DISCOUNT = rand.nextBoolean() ? 0 : rand.nextFloat() * 10;
                    //insertInSuperColumn(OL_DISCOUNT, O_ID + "", subcolumn_family, OL_ID, "OL_DISCOUNT", write_con);

                    OL_COMMENT = null;

                    if (rand.nextBoolean()) {
                        OL_COMMENT = BenchmarkUtil.getRandomAString(20, 100);
                        //insertInSuperColumn(OL_COMMENT, O_ID + "", subcolumn_family, OL_ID, "OL_COMMENT", write_con);
                    }
                    OrderLine orderline = new OrderLine(OL_ID, order.getO_ID(), OL_I_ID, OL_QTY, OL_DISCOUNT, OL_COMMENT);
                    databaseInsert("INSERT Orders Lines", O_ID + "", subcolumn_family, order,partial_results);
                    //orderLines.add(orderline);
                }



                String CX_TYPE;
                int CX_NUM;
                String CX_NAME;
                java.sql.Date CX_EXPIRY;
                double CX_XACT_AMT; //O_TOTAL
                int CX_CO_ID; //Order.getID;

                column_family = "CC_XACTS";

                CX_NUM = BenchmarkUtil.getRandomNString(16);
                String key = O_ID + "";

                CX_TYPE = credit_cards[BenchmarkUtil.getRandomInt(0, credit_cards.length - 1)];
                //insert(CX_TYPE, key, column_family, "CX_TYPE", write_con);


                //insert(CX_NUM, key, column_family, "CX_NUM", write_con);

                CX_NAME = BenchmarkUtil.getRandomAString(14, 30);
                //insert(CX_NAME, key, column_family, "CX_NAME", write_con);

                GregorianCalendar cal = new GregorianCalendar();
                cal.add(Calendar.DAY_OF_YEAR, BenchmarkUtil.getRandomInt(10, 730));
                CX_EXPIRY = new java.sql.Date(cal.getTime().getTime());
                //insert(CX_EXPIRY, key, column_family, "CX_EXPIRY", write_con);

                //DATE
//            insert(O_SHIP_DATE, key, column_family, "CX_XACT_DATE", write_con);

                //AMOUNT
                //          insert(O_TOTAL, key, column_family, "CX_XACT_AMT", write_con);

                //CX_AUTH_ID = getRandomAString(5,15);// unused
                String country_id = countries.get(BenchmarkUtil.getRandomInt(0, countries.size() - 1));
                //        insert(country.getCo_id(), key, column_family, "CX_CO_ID", write_con);



                CCXact ccXact = new CCXact(CX_TYPE, CX_NUM, CX_NAME, CX_EXPIRY,/* CX_AUTH_ID,*/ O_TOTAL,
                        O_SHIP_DATE, /* 1 + _counter, */ order.getO_ID(), country_id);

                databaseInsert("INSERT CCXact", key, column_family, order,partial_results);

                O_ID++;
            }
            if (debug) {
                System.out.println("Thread finished: " + number_keys + " orders and xact inserted. key:" + begin_key);
            }

            barrier.countDown();

        }

        public ResultHandler returnResults() {
            return partial_results;
        }
    }

    /************************************************************************/
    /************************************************************************/
    /************************************************************************/
//    public void ShowValues(String column_family) {
//
//        System.out.println("Show Values for Column family : " + column_family);
//        try {
//            SlicePredicate predicate = new SlicePredicate();
//            SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, 300);
//
//            // ColumnParent parent = new ColumnParent(column_family, null);
//            ColumnParent parent = new ColumnParent(column_family);
//            predicate.setSlice_range(range);
//
//            //List<String> keys = client..get_key_range(Keyspace, column_family, "", "", 300, ConsistencyLevel.ONE);
//            List<KeySlice> keys = client.get_range_slice(Keyspace, parent, predicate, "", "", 300, ConsistencyLevel.ONE);
//            System.out.println("Keys size: " + keys.size());
//            for (KeySlice ks : keys) {
//                System.out.println("For key: " + ks.key);
//                List<ColumnOrSuperColumn> line = ks.columns;
//                StringBuilder builder = new StringBuilder();
//                for (ColumnOrSuperColumn column : line) {
//
//                    Column c = column.getColumn();
//
//                    builder.append(" Name: " + new String(c.getName()) + " Value: " + new String(c.getValue()) + "||");
//                }
//                System.out.println("Key on column: " + ks.key + "with values: " + builder.toString());
//
//            }
//
//
////            Map<String, List<ColumnOrSuperColumn>> results = client.multiget_slice(Keyspace, keys, parent, predicate, ConsistencyLevel.ONE);
////            for (String key : results.keySet()) {
////                List<ColumnOrSuperColumn> line = results.get(key);
////                StringBuilder builder = new StringBuilder();
////                for (ColumnOrSuperColumn column : line) {
////
////                    Column c = column.getColumn();
////
////                    builder.append(" Name: " + new String(c.getName()) + " Value: " + new String(c.getValue()) + "||");
////                }
////                System.out.println("Key on column: " + key + "with values: " + builder.toString());
////            }
//        } catch (TimedOutException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (InvalidRequestException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (UnavailableException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        }
//    }
    public static boolean loadDescriptor() {
        try {


            FileInputStream in = null;
            String jsonString_r = "";
            try {

                in = new FileInputStream("CassandraDB.xml");
                BufferedReader bin = new BufferedReader(new InputStreamReader(in));
                String s = "";
                StringBuilder sb = new StringBuilder();
                while (s != null) {
                    sb.append(s);
                    s = bin.readLine();
                }

                jsonString_r = sb.toString().replace("\n", "");
                bin.close();
                in.close();

            } catch (FileNotFoundException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);

            } finally {
                try {
                    in.close();
                } catch (IOException ex) {
                    Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
                }
            }


            Map<String, Map<String, String>> map = JsonUtil.getMapMapFromJsonString(jsonString_r);


            if (!map.containsKey("BenchmarkInfo")) {
                System.out.println("ERROR: NO INFORMATION ABOUT THE DATA ENGINE FOUND, ABORTING");
                return false;
            } else {

                Map<String, String> databaseInfo = map.get("BenchmarkInfo");
                String databaseClass = databaseInfo.get("DataEngineInterface");
                System.out.println("CHOSEN DATABASE ENGINE: " + databaseClass);
                databaseClient = (CRUD) Class.forName(databaseClass).getConstructor().newInstance();

                if (!map.containsKey("LoadParameters")) {
                    System.out.println("WARNING: NO LOAD DEFINITIONS FOUND, NO DELAYS WILL BE USED , ONE THREAD USED");
                } else {
                    Map<String, String> loadInfo = map.get("LoadParameters");
                    String do_delays = loadInfo.get("delay_inserts");
                    delay_inserts = Boolean.valueOf(do_delays);
                    String delay_time_info = loadInfo.get("delay_time");
                    delay_time = Integer.valueOf(delay_time_info);
                    String thread_number_info = loadInfo.get("thread_number");
                    num_threads = Integer.parseInt(thread_number_info);
                }
                return true;
            }
        } catch (NoSuchMethodException ex) {
            Logger.getLogger(BenchmarkPopulator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SecurityException ex) {
            Logger.getLogger(BenchmarkPopulator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            Logger.getLogger(BenchmarkPopulator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(BenchmarkPopulator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalArgumentException ex) {
            Logger.getLogger(BenchmarkPopulator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InvocationTargetException ex) {
            Logger.getLogger(BenchmarkPopulator.class.getName()).log(Level.SEVERE, null, ex);

        } catch (ClassNotFoundException ex) {
            Logger.getLogger(BenchmarkPopulator.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("ERROR: THERE IS SOME PROBLEM WITH THE DEFINITIONS FILE");
        return false;
    }

    public void info() {
//        Namegenerator ng = new Namegenerator("src/JaNaG_Source/languages.txt", "src/JaNaG_Source/semantics.txt");
//        //String[] ns  =  ng.getPatterns();
//        String[] ns = ng.getGenders("Orkisch");
//        for (String ss : ns) {
//            System.out.println(ss);
//
//        }
        /**
        Patterns:

        Pseudo-Altdeutsch
        Menschlich Fantasy (Mann , Frau)
        Götternamen
        Dämonen
        Griechisch
        Spanisch
        Italienisch
        Irisch
        Französisch
        Polnisch
        Hebräisch (Mann , Frau)
        Orkisch
        US-Zensus//english
         *
         */
//
//        try {
//            Map<String, Map<String, String>> info = client.describe_keyspace("Tpcw");
//            System.out.println("Info =" + info.toString());
//
//        } catch (NotFoundException ex) {
//            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TException ex) {
//            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
//        }
    }
}
/**
try {
ColumnOrSuperColumn scolumn = client.get(Keyspace, "all", path, write_con);
SuperColumn super_column = scolumn.getSuper_column();
Column column = super_column.columns.get(0);
O_ID = (Long) BenchmarkUtil.toObject(column.value) + 1;
insertInSuperColumn(O_ID, "all", column_family, "ids", "LAST_ID", write_con);
} catch (TimedOutException ex) {
Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
} catch (InvalidRequestException ex) {
Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
O_ID = 0;
} catch (NotFoundException ex) {
O_ID = 0;
insertInSuperColumn(O_ID, "all", column_family, "ids", "LAST_ID", write_con);
Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
} catch (UnavailableException ex) {
Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
} catch (TException ex) {
Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
}

 *
 *
 *
 *
try {

ColumnOrSuperColumn scolumn = client.get(Keyspace, "all", path, write_con);
SuperColumn super_column = scolumn.getSuper_column();
Column column = super_column.columns.get(0);
O_ID = (Long) BenchmarkUtil.toObject(column.value);
O_ID++;
//new Long(new String(column.value)) + 1;
insertInSuperColumn(O_ID, "all", "Orders", "ids", "LAST_ID", write_con);

 **/
