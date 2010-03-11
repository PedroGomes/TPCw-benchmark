package test;

///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//
///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package cassandratest;
//
//import CassandraEntities.Address;
//import CassandraEntities.Author;
//import CassandraEntities.CCXact;
//import CassandraEntities.Item;
//import CassandraEntities.Orders;
//import CassandraEntities.Costumer;
//import CassandraEntities.Country;
//import CassandraEntities.OrderLine;
//import JaNaG_Source.de.beimax.janag.Namegenerator;
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.io.ObjectOutputStream;
//import java.util.ArrayList;
//import java.util.Calendar;
//import java.util.GregorianCalendar;
//import java.util.List;
//import java.util.Map;
//import java.util.Random;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//
//
//import org.apache.cassandra.service.Cassandra.Client;
//import org.apache.cassandra.service.Column;
//import org.apache.cassandra.service.ColumnOrSuperColumn;
//import org.apache.cassandra.service.ColumnParent;
//import org.apache.cassandra.service.ColumnPath;
//import org.apache.cassandra.service.ConsistencyLevel;
//import org.apache.cassandra.service.InvalidRequestException;
//import org.apache.cassandra.service.NotFoundException;
//import org.apache.cassandra.service.SlicePredicate;
//import org.apache.cassandra.service.SuperColumn;
//import org.apache.cassandra.service.SliceRange;
//
//
//import org.apache.cassandra.service.UnavailableException;
//import org.apache.thrift.TException;
//import org.apache.thrift.protocol.TBinaryProtocol;
//import org.apache.thrift.protocol.TProtocol;
//import org.apache.thrift.transport.TSocket;
//import org.apache.thrift.transport.TTransportException;
//
///**
// *
// * @author pedro
// */
//public class PopulateDatabase {
//
//    /**Time messuerments*/
//    public int simple_time = 0;
//    public long simple_inserts = 0;
//    public int super_time = 0;
//    public long super_inserts = 0;
//
//    //ATTENTION: The NUM_EBS and NUM_ITEMS variables are the only variables
//    //that should be modified in order to rescale the DB.
//    private static /* final */ int NUM_EBS = 10;
//    private static /* final */ int NUM_ITEMS = 1000;
//
//    private static /* final */ int NUM_CUSTOMERS = NUM_EBS * 2880;
//    private static /* final */ int NUM_ADDRESSES = 2 * NUM_CUSTOMERS;
//    private static /* final */ int NUM_AUTHORS = (int) (.25 * NUM_ITEMS);
//    private static /* final */ int NUM_ORDERS = (int) (.9 * NUM_CUSTOMERS);
//    private static /* final */ int NUM_COUNTRIES = 92; // this is constant. Never changes!
//
//
//    private static Random rand;
//
//    static Client client;
//    public String Keyspace = "Tpcw";
//
//
//    ArrayList<Author> authors = new ArrayList<Author>();
//    ArrayList<Address> addresses =  new ArrayList<Address>();
//    ArrayList<Country> countries =  new ArrayList<Country>();
//
//    ArrayList<Costumer> costumers = new ArrayList<Costumer>();
//    ArrayList<Item> items = new ArrayList<Item>();
//    ArrayList<Orders> orders = new ArrayList<Orders>();
//    ArrayList<OrderLine> orderLines = new ArrayList<OrderLine>();
//
//
//    String[] subjects = {"ARTS", "BIOGRAPHIES", "BUSINESS", "CHILDREN",
//        "COMPUTERS", "COOKING", "HEALTH", "HISTORY",
//        "HOME", "HUMOR", "LITERATURE", "MYSTERY",
//        "NON-FICTION", "PARENTING", "POLITICS",
//        "REFERENCE", "RELIGION", "ROMANCE",
//        "SELF-HELP", "SCIENCE-NATURE", "SCIENCE-FICTION",
//        "SPORTS", "YOUTH", "TRAVEL"};
//    String[] backings = {"HARDBACK", "PAPERBACK", "USED", "AUDIO",
//        "LIMITED-EDITION"};
//
//    /**
//     * @param args the command line arguments
//     */
//    public static void main(String[] args) {
//        try {
//            int port = 9160;
//            TSocket socket = new TSocket("localhost", port);
//            TProtocol prot = new TBinaryProtocol(socket);
//            client = new Client(prot);
//            socket.open();
//        } catch (TTransportException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        }
//
//        rand =  new Random();
//
//        PopulateDatabase m = new PopulateDatabase();
//        m.removeALL();
//        //m.populate(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM);
//        //m.ShowValues("Costumer");
//        //m.info();
//
//
//    }
//
//
//
//    public void populate(int writeQuorum, int readQuorum) {
//
//        insertCountries(NUM_COUNTRIES, writeQuorum);
//        insertAddress(NUM_ADDRESSES, writeQuorum);
//        insertCostumers(NUM_CUSTOMERS, writeQuorum);
//        insertAuthors(NUM_AUTHORS, writeQuorum);
//        insertItems(NUM_ITEMS, writeQuorum);
//        insertOrder_and_CC_XACTS(NUM_ORDERS, writeQuorum);
//
//    }
//
//    public void removeALL() {
//
//        removeRows("Costumer", NUM_CUSTOMERS , ConsistencyLevel.QUORUM);
//        removeRows("Items", NUM_ITEMS, ConsistencyLevel.QUORUM);
//        removeRows("Orders", NUM_ORDERS, ConsistencyLevel.QUORUM);
//        removeRows("OrderLines", NUM_ORDERS*5, ConsistencyLevel.QUORUM);
//        removeRows("Author", NUM_AUTHORS,ConsistencyLevel.QUORUM);
//    }
//
//
//
//    public void insert(Object value, String key, String column_family, String column, int writeConsistency) {
//        try {
//            ColumnPath path = new ColumnPath(column_family, null, null);
//            path.setColumn(column.getBytes());
//            byte[] valueBytes = getBytes(value);
//            long time = System.currentTimeMillis();
//            client.insert(Keyspace, key, path, valueBytes, System.currentTimeMillis(), writeConsistency);
//            time = System.currentTimeMillis() - time;
//            simple_time += time;
//            simple_inserts ++;
//            // System.out.println("Time to insert: " + time);
//        } catch (InvalidRequestException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (UnavailableException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        }
//    }
//
//    public void insertInSuperColumn(Object value, String key, String column_family, String SuperColumn, String Column, int writeConsistency) {
//        try {
//            ColumnPath path = new ColumnPath(column_family, SuperColumn.getBytes(), Column.getBytes());
//            byte[] valueBytes = getBytes(value);
//            long time = System.currentTimeMillis();
//            client.insert(Keyspace, key, path, valueBytes, System.currentTimeMillis(), writeConsistency);
//            time = System.currentTimeMillis() - time;
//            super_time += time;
//            super_inserts++;
//            //System.out.println("Time to insert: " + time);
//        } catch (InvalidRequestException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (UnavailableException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        }
//
//    }
//
//    public void remove(String key, String column, int writeConsistency) {
//        try {
//            ColumnPath path = new ColumnPath(column, null, null);
//            client.remove(Keyspace, key, path, System.currentTimeMillis(), writeConsistency);
//        } catch (InvalidRequestException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (UnavailableException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        }
//    }
//
//    public void update() {
//    }
//
//    public void insertAuthors(int n, int writeConsistency){
//
//        Namegenerator ng = new Namegenerator("src/JaNaG_Source/languages.txt", "src/JaNaG_Source/semantics.txt");
//
//        for (int i = 0; i < n; i++) {
//            GregorianCalendar cal = getRandomDate(1800, 1990);
//
//            String[] names = (ng.getRandomName("US-Zensus", "Männlich Top 500+")).split(" ");
//            String[] Mnames =(ng.getRandomName("US-Zensus", "Männlich Top 500+")).split(" ");
//
//            String first_name = names[0];
//            String last_name = names[1];
//            String middle_name = Mnames[1];
//            java.sql.Date dob = new java.sql.Date(cal.getTime().getTime());
//            String bio  =  getRandomAString(125, 500);
//            String key = first_name+middle_name+last_name+rand.nextInt(1000);
//
//            insert(first_name, key, "Author", "A_FNAME", writeConsistency);
//            insert(last_name, key, "Author", "A_LNAME", writeConsistency);
//            insert(middle_name, key, "Author", "A_MNAME",writeConsistency);
//            insert(dob, key, "Author", "A_DOB", writeConsistency);
//            insert(bio, key, "Author", "A_BIO", writeConsistency);
//
//            Author a = new Author(first_name, last_name, middle_name, dob, bio, key);
//            authors.add(a);
//        }
//    }
//
//
//
//    public void insertCostumers(int n, int writeCon) {
//
//        Namegenerator ng = new Namegenerator("src/JaNaG_Source/languages.txt", "src/JaNaG_Source/semantics.txt");
//
//        for (int i = 0; i < n; i++) {
//
//            String name = ng.getRandomName("US-Zensus", "Männlich Top 500+");
//            String[] names = name.split(" ");
//            Random r = new Random();
//            int random_int = r.nextInt(1000);
//
//            String key = name + "_" + random_int;
//
//            String pass = names[0].charAt(0) + names[1].charAt(0) + "" + random_int;
//            insert(pass, key, "Costumer", "C_PASSWD", writeCon);
//
//            String first_name = names[0];
//            insert(first_name, key, "Costumer", "C_FNAME", writeCon);
//
//            String last_name = names[1];
//            insert(last_name, key, "Costumer", "C_LNAME", writeCon);
//
//            int phone = r.nextInt(999999999 - 100000000) + 100000000;
//            insert(phone, key, "Costumer", "C_PHONE", writeCon);
//
//            String email = key + "@email.com";
//            insert(email, key, "Costumer", "C_EMAIL", writeCon);
//
//            double discount = r.nextDouble();
//            insert(discount, key, "Costumer", "C_DISCOUNT", writeCon);
//
//            String adress = "Street: " + ng.getRandomName("US-Zensus", "Weiblich Ungewöhnlich") + " number: " + r.nextInt(500);
//            insert(adress, key, "Costumer", "C_PHONE", writeCon);
//
//
//            double C_BALANCE=0.00;
//            insert(C_BALANCE, key, "Costumer", "C_BALANCE", writeCon);
//
//	    double C_YTD_PMT = (double) getRandomInt(0, 99999)/100.0;
//            insert(C_YTD_PMT, key, "Costumer", "C_YTD_PMT", writeCon);
//
//            GregorianCalendar cal = new GregorianCalendar();
//	    cal.add(Calendar.DAY_OF_YEAR,-1*getRandomInt(1,730));
//
//            java.sql.Date C_SINCE = new java.sql.Date(cal.getTime().getTime());
//            insert(C_SINCE , key, "Costumer", "C_SINCE ", writeCon);
//
//            cal.add(Calendar.DAY_OF_YEAR,getRandomInt(0,60));
//	    if(cal.after(new GregorianCalendar()))
//		cal = new GregorianCalendar();
//
//	    java.sql.Date C_LAST_LOGIN = new java.sql.Date(cal.getTime().getTime());
//            insert(C_LAST_LOGIN , key, "Costumer", "C_LAST_LOGIN", writeCon);
//
//            java.sql.Timestamp C_LOGIN = new java.sql.Timestamp(System.currentTimeMillis());
//            insert(C_LOGIN , key, "Costumer", "C_LOGIN", writeCon);
//
//            cal = new GregorianCalendar();
//	    cal.add(Calendar.HOUR, 2);
//
//	    java.sql.Timestamp C_EXPIRATION = new java.sql.Timestamp(cal.getTime().getTime());
//            insert(C_EXPIRATION , key, "Costumer", "C_EXPIRATION", writeCon);
//
//            cal = getRandomDate(1880, 2000);
//	    java.sql.Date C_BIRTHDATE = new java.sql.Date(cal.getTime().getTime());
//            insert(C_BIRTHDATE, key, "Costumer", "C_BIRTHDATE", writeCon);
//
//	    String C_DATA = getRandomAString(100,500);
//            insert(C_DATA, key, "Costumer", "C_DATA", writeCon);
//
//            Address address = addresses.get(rand.nextInt(addresses.size()));
//            insert(address.getAddr_id(), key, "Costumer", "C_ADDR_ID", writeCon);
//
//            Costumer c = new Costumer(key, pass, last_name, first_name, phone, email, C_SINCE, C_LAST_LOGIN, C_LOGIN, C_EXPIRATION, C_BALANCE, C_YTD_PMT,C_BIRTHDATE , C_DATA, discount,address);
//
//            costumers.add(c);
//        }
//    }
//
//    public void insertItems(int n, int writeCon) {
//
//        Namegenerator ng = new Namegenerator("src/JaNaG_Source/languages.txt", "src/JaNaG_Source/semantics.txt");
//
//
//        String column_family = "Items";
//
//        ArrayList<String> titles = new ArrayList<String>();
//        for (int i = 0; i < n; i++) {
//            boolean rad1 = rand.nextBoolean();
//            String f_name = rad1 ? ng.getRandomName("Menschlich Fantasy", "Mann") : ng.getRandomName("Menschlich Fantasy", "Frau");
//            String l_name = rad1 ? ng.getRandomName("Hebräisch", "Mann") : ng.getRandomName("Hebräisch", "Frau");
//            int num = rand.nextInt(1000);
//            titles.add(f_name + " and " + l_name + " " + num);
//        }
//
//        for (int i = 0; i < n; i++) {
//            String I_TITLE; //ID
//            String I_AUTHOR;
//            String I_PUBLISHER;
//            String I_DESC;
//            String I_SUBJECT;
//            float I_COST;
//            long I_STOCK;
//            List<String> I_RELATED = new ArrayList<String>();
//            int I_PAGE;
//            String I_BACKING;
//
//            I_TITLE = titles.get(i);
//
//
//            boolean rad1 = rand.nextBoolean();
//            I_AUTHOR = rad1 ? ng.getRandomName("Götternamen", "Gott") : ng.getRandomName("Götternamen", "Göttin");
//            insert(I_AUTHOR, I_TITLE, column_family, "I_AUTHOR", writeCon);
//
//
//            rad1 = rand.nextBoolean();
//            I_PUBLISHER = rad1 ? ng.getRandomName("Polnisch", "Mann") : ng.getRandomName("Polnisch", "Frau");
//            insert(I_PUBLISHER, I_TITLE, column_family, "I_PUBLISHER", writeCon);
//
//            rad1 = rand.nextBoolean();
//            I_DESC = null;
//            if (rad1) {
//                boolean rad2 = rand.nextBoolean();
//                I_DESC = rad2 ? ng.getRandomName("Spanisch", "Mann") : ng.getRandomName("Spanisch", "Frau");
//                insert(I_DESC, I_TITLE, column_family, "I_DESC", writeCon);
//            }
//
//            I_COST = rand.nextInt(100);
//            insert(I_AUTHOR, I_TITLE, column_family, "I_AUTHOR", writeCon);
//
//            I_STOCK = rand.nextInt(100);
//            insert(I_STOCK, I_TITLE, column_family, "I_STOCK", writeCon);
//
//            int related_number = rand.nextInt(5);
//            for (int z = 0; z < related_number; z++) {
//                String title = titles.get(rand.nextInt(n));
//                if (!I_RELATED.contains(title)) {
//                    I_RELATED.add(title);
//                }
//            }
//            if (related_number > 0) {
//                insert(I_RELATED, I_TITLE, column_family, "I_RELATED", writeCon);
//            }
//
//            I_PAGE = rand.nextInt(500) + 10;
//            insert(I_PAGE, I_TITLE, column_family, "I_PAGE", writeCon);
//
//            I_SUBJECT = subjects[rand.nextInt(subjects.length - 1)];
//            insert(I_SUBJECT, I_TITLE, column_family, "I_SUBJECT", writeCon);
//
//            I_BACKING = backings[rand.nextInt(backings.length - 1)];
//            insert(I_BACKING, I_TITLE, column_family, "I_BACKING", writeCon);
//
//
//            Item item = new Item(I_TITLE, I_AUTHOR, I_PUBLISHER, I_DESC, I_SUBJECT, I_COST, I_STOCK, I_RELATED, I_PAGE, I_BACKING);
//            items.add(item);
//        }
//    }
//
//    public void insertOrder_and_CC_XACTS(int number_keys, int write_con) {
//        String column_family = "Orders";
//        String subcolumn_family = "OrderLines";
//        Namegenerator ng = new Namegenerator("src/JaNaG_Source/languages.txt", "src/JaNaG_Source/semantics.txt");
//        String[] credit_cards = {"VISA", "MASTERCARD", "DISCOVER", "AMEX", "DINERS"};
//        String[] ship_types = {"AIR", "UPS", "FEDEX", "SHIP", "COURIER", "MAIL"};
//        String[] status_types = {"PROCESSING", "SHIPPED", "PENDING", "DENIED"};
//
//        long O_ID = 0;
//        ColumnPath path = new ColumnPath(column_family, "ids".getBytes(), null);
//        try {
//             ColumnOrSuperColumn scolumn = client.get(Keyspace, "all", path, write_con);
//            SuperColumn super_column = scolumn.getSuper_column();
//            Column column = super_column.columns.get(0);
//            O_ID = new Long(new String(column.value)) + 1;
//            insertInSuperColumn(O_ID, "all", column_family, "ids", "LAST_ID", write_con);
//
//        } catch (InvalidRequestException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//            O_ID = 0;
//        } catch (NotFoundException ex) {
//            O_ID = 0;
//            insertInSuperColumn(O_ID, "all", column_family, "ids", "LAST_ID", write_con);
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (UnavailableException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        }
//
//        for (int z = 0; z < number_keys; z++) {
//
//
//            String O_C_ID;
//            java.sql.Timestamp O_DATE;
//            float O_SUB_TOTAL;
//            float O_TAX;
//            float O_TOTAL;
//            java.sql.Timestamp O_SHIP_DATE;
//            String O_SHIP_TYPE;
//            Address O_SHIP_ADDR;
//            String O_STATUS;
//
//            try {
//
//                ColumnOrSuperColumn scolumn = client.get(Keyspace, "all", path, write_con);
//                SuperColumn super_column = scolumn.getSuper_column();
//                Column column = super_column.columns.get(0);
//                O_ID =  (Long) toObject(column.value);
//                O_ID++;
//                //new Long(new String(column.value)) + 1;
//                insertInSuperColumn(O_ID, "all", column_family, "ids", "LAST_ID", write_con);
//
//            } catch (InvalidRequestException ex) {
//                Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//                O_ID = 0;
//            } catch (NotFoundException ex) {
//                O_ID = 0;
//                Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//            } catch (UnavailableException ex) {
//                Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//            } catch (TException ex) {
//                Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//            }
//
//            Costumer c = costumers.get(rand.nextInt(costumers.size()));
//
//            O_C_ID = c.getC_UNAME();
//
//            GregorianCalendar call = new GregorianCalendar();
//            O_DATE = new java.sql.Timestamp(call.getTime().getTime());
//            insertInSuperColumn(O_DATE, O_C_ID, column_family, O_ID + "", "O_DATE", write_con);
//
//            O_SUB_TOTAL = rand.nextFloat() * 100 * 4;
//            insertInSuperColumn(O_SUB_TOTAL, O_C_ID, column_family, O_ID + "", "O_SUB_TOTAL", write_con);
//
//            O_TAX = O_SUB_TOTAL * 0.21f;
//            insertInSuperColumn(O_TAX, O_C_ID, column_family, O_ID + "", "O_TAX", write_con);
//
//            O_TOTAL = O_SUB_TOTAL + O_TAX;
//            insertInSuperColumn(O_TOTAL, O_C_ID, column_family, O_ID + "", "O_TOTAL", write_con);
//
//            call.add(Calendar.DAY_OF_YEAR, getRandomInt(0,7));
//            O_SHIP_DATE = new java.sql.Timestamp(call.getTime().getTime());
//            insertInSuperColumn(O_SHIP_DATE, O_C_ID, column_family, O_ID + "", "O_SHIP_DATE", write_con);
//
//            O_SHIP_TYPE = ship_types[rand.nextInt(ship_types.length)];
//            insertInSuperColumn(O_SHIP_TYPE, O_C_ID, column_family, O_ID + "", "O_SHIP_TYPE", write_con);
//
//            O_STATUS = status_types[rand.nextInt(status_types.length)];
//            insertInSuperColumn(O_STATUS, O_C_ID, column_family, O_ID + "", "O_STATUS", write_con);
//
//            Address billAddress = addresses.get(getRandomInt(0, NUM_ADDRESSES));
//            insertInSuperColumn(billAddress.getAddr_id(), O_C_ID, column_family, O_ID + "", "O_BILL_ADDR_ID", write_con);
//
//
//	    O_SHIP_ADDR = addresses.get(getRandomInt(0, NUM_ADDRESSES));
//            insertInSuperColumn(O_SHIP_ADDR.getAddr_id(), O_C_ID, column_family, O_ID + "", "O_SHIP_ADDR_ID", write_con);
//
//            int number_of_items = rand.nextInt(4) + 1;
//
//            for (int i = 0; i < number_of_items; i++) {
//                /**
//                 * OL_ID
//                 * OL_O_ID
//                 * OL_I_ID
//                 * OL_QTY
//                 * OL_DISCOUNT
//                 * OL_COMMENT
//                 */
//                String OL_ID;
//                long OL_O_ID = O_ID;
//                String OL_I_ID;
//                int OL_QTY;
//                float OL_DISCOUNT;
//                String OL_COMMENT;
//
//                OL_ID = O_ID + "N" + i;
//
//
//                OL_I_ID = items.get(rand.nextInt(items.size())).getI_TITLE();
//                insertInSuperColumn(OL_I_ID, O_ID + "", subcolumn_family, OL_ID, "OL_I_ID", write_con);
//
//                OL_QTY = rand.nextInt(4) + 1;
//                insertInSuperColumn(OL_QTY, O_ID + "", subcolumn_family, OL_ID, "OL_QTY", write_con);
//
//                OL_DISCOUNT = rand.nextBoolean() ? 0 : rand.nextFloat() * 10;
//                insertInSuperColumn(OL_DISCOUNT, O_ID + "", subcolumn_family, OL_ID, "OL_DISCOUNT", write_con);
//
//                OL_COMMENT = null;
//
//                if (rand.nextBoolean()) {
//                    OL_COMMENT = getRandomAString(20,100);
//                    insertInSuperColumn(OL_COMMENT, O_ID + "", subcolumn_family, OL_ID, "OL_COMMENT", write_con);
//                }
//                OrderLine orderline = new OrderLine(OL_ID, OL_I_ID, OL_I_ID, OL_QTY, OL_DISCOUNT, OL_COMMENT);
//                orderLines.add(orderline);
//            }
//
//            Orders order = new Orders(O_ID, O_C_ID, O_DATE, O_SUB_TOTAL, O_TAX, O_TOTAL, O_SHIP_TYPE, O_SHIP_DATE, O_STATUS, billAddress,O_SHIP_ADDR);
//            orders.add(order);
//
//
//        String CX_TYPE;
//	int CX_NUM;
//	String CX_NAME;
//	java.sql.Date CX_EXPIRY;
//	double CX_XACT_AMT;
//	int CX_CO_ID;
//
//        column_family = "CC_XACTS";
//
//            CX_NUM = getRandomNString(16);
//            String key =  O_ID+CX_NUM+"";
//
//            CX_TYPE = credit_cards[getRandomInt(0, credit_cards.length - 1)];
//            insert(CX_TYPE, key, column_family, "CX_TYPE", write_con);
//
//
//            insert(CX_NUM, key, column_family, "CX_NUM", write_con);
//
//	    CX_NAME = getRandomAString(14,30);
//            insert(CX_NAME, key, column_family, "CX_NAME", write_con);
//
//	    GregorianCalendar cal = new GregorianCalendar();
//	    cal.add(Calendar.DAY_OF_YEAR, getRandomInt(10, 730));
//	    CX_EXPIRY = new java.sql.Date(cal.getTime().getTime());
//            insert(CX_EXPIRY, key, column_family, "CX_EXPIRY", write_con);
//
//            //DATE
//            insert(O_SHIP_DATE, key, column_family, "CX_XACT_DATE", write_con);
//
//            //AMOUNT
//            insert(O_TOTAL, key, column_family, "CX_XACT_AMT", write_con);
//
//	    //CX_AUTH_ID = getRandomAString(5,15);// unused
//	    Country country = countries.get(getRandomInt(0,countries.size()));
//            insert(country.getCo_id(), key, column_family, "CX_CO_ID", write_con);
//
//
//
//	    CCXact ccXact = new CCXact(CX_TYPE, CX_NUM, CX_NAME, CX_EXPIRY,/* CX_AUTH_ID,*/ O_TOTAL,
//				       O_SHIP_DATE, /* 1 + _counter, */order, country);
//
//        }
//
//    }
//
//    public void removeRows(String column_family, int number_keys, int write_con) {
//        System.out.println("Removing ColumnFamily:"+column_family);
//        try {
//
//            SlicePredicate predicate = new SlicePredicate();
//            SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, number_keys);
//            ColumnParent parent = new ColumnParent(column_family, null);
//            predicate.setSlice_range(range);
//            List<String> keys = client.get_key_range(Keyspace, column_family, "", "", number_keys, ConsistencyLevel.ONE);
//            // Map<String, List<ColumnOrSuperColumn>> results = client.multiget_slice(Keyspace, keys, parent, predicate, ConsistencyLevel.ONE);
//            ColumnPath path = new ColumnPath(column_family, null, null);
//            for (String key : keys) {
//                client.remove(Keyspace, key, path, System.currentTimeMillis(), write_con);
//            }
//        } catch (InvalidRequestException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (UnavailableException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        }
//    }
//
//
//    private void insertAddress(int n, int writeConsistency){
//
//	String ADDR_STREET1, ADDR_STREET2, ADDR_CITY, ADDR_STATE;
//	String ADDR_ZIP;
//	Country country;
//
//	for (int i = 0; i < n; i++) {
//	    ADDR_STREET1 = "street" + getRandomAString(10,30);
//
//
//	    ADDR_STREET2 = "street" + getRandomAString(10,30);
//	    ADDR_CITY    = getRandomAString(4,30);
//	    ADDR_STATE   = getRandomAString(2,20);
//	    ADDR_ZIP     = getRandomAString(5,10);
//	    country      = countries.get(getRandomInt(0, NUM_COUNTRIES - 1));
//
//	    Address address = new Address(ADDR_STREET1, ADDR_STREET2, ADDR_CITY,
//					  ADDR_STATE, ADDR_ZIP, country);
//            String key = address.getAddr_id();
//            insert(ADDR_STREET1, key, "Addresses", "ADDR_STREET1", writeConsistency);
//            insert(ADDR_STREET2, key, "Addresses", "ADDR_STREET2", writeConsistency);
//            insert(ADDR_STATE, key, "Addresses", "ADDR_STATE", writeConsistency);
//            insert(ADDR_CITY, key, "Addresses", "ADDR_CITY", writeConsistency);
//            insert(ADDR_ZIP, key, "Addresses", "ADDR_ZIP", writeConsistency);
//            insert(country.getCo_id(), key, "Addresses", "ADDR_CO_ID", writeConsistency);
//            addresses.add(address);
//	}
//  }
//
//
//
//    private void insertCountries(int numCountries, int writeConsitency) {
//
//	String[] countriesNames = {
//	    "United States","United Kingdom","Canada", "Germany", "France","Japan",
//	    "Netherlands","Italy","Switzerland","Australia","Algeria","Argentina",
//	    "Armenia","Austria","Azerbaijan","Bahamas","Bahrain","Bangla Desh",
//	    "Barbados","Belarus","Belgium","Bermuda", "Bolivia","Botswana","Brazil",
//	    "Bulgaria","Cayman Islands","Chad", "Chile", "China","Christmas Island",
//	    "Colombia","Croatia","Cuba","Cyprus","Czech Republic","Denmark",
//	    "Dominican Republic","Eastern Caribbean","Ecuador","Egypt","El Salvador",
//	    "Estonia","Ethiopia","Falkland Island","Faroe Island", "Fiji","Finland",
//	    "Gabon","Gibraltar","Greece","Guam","Hong Kong","Hungary","Iceland",
//	    "India","Indonesia","Iran","Iraq","Ireland","Israel","Jamaica", "Jordan",
//	    "Kazakhstan","Kuwait","Lebanon","Luxembourg","Malaysia","Mexico",
//	    "Mauritius", "New Zealand","Norway","Pakistan","Philippines","Poland",
//	    "Portugal","Romania","Russia","Saudi Arabia","Singapore","Slovakia",
//	    "South Africa","South Korea", "Spain","Sudan","Sweden","Taiwan",
//	    "Thailand","Trinidad","Turkey","Venezuela", "Zambia"
//	};
//
//	double[] exchanges = {
//	    1, .625461, 1.46712, 1.86125, 6.24238, 121.907, 2.09715, 1842.64, 1.51645,
//	    1.54208, 65.3851, 0.998, 540.92, 13.0949, 3977, 1, .3757, 48.65, 2, 248000,
//	    38.3892, 1, 5.74, 4.7304, 1.71, 1846, .8282, 627.1999, 494.2, 8.278,
//	    1.5391, 1677, 7.3044, 23, .543, 36.0127, 7.0707, 15.8, 2.7, 9600, 3.33771,
//	    8.7, 14.9912, 7.7, .6255, 7.124, 1.9724, 5.65822, 627.1999, .6255, 309.214,
//	    1, 7.75473, 237.23, 74.147, 42.75, 8100, 3000, .3083, .749481, 4.12, 37.4,
//	    0.708, 150, .3062, 1502, 38.3892, 3.8, 9.6287, 25.245, 1.87539, 7.83101, 52,
//	    37.8501, 3.9525, 190.788, 15180.2, 24.43, 3.7501, 1.72929, 43.9642, 6.25845,
//	    1190.15, 158.34, 5.282, 8.54477, 32.77, 37.1414, 6.1764, 401500, 596, 2447.7
//	};
//
//	String[] currencies = {
//	    "Dollars","Pounds","Dollars","Deutsche Marks","Francs","Yen","Guilders",
//	    "Lira","Francs","Dollars","Dinars","Pesos", "Dram","Schillings","Manat",
//	    "Dollars","Dinar","Taka","Dollars","Rouble","Francs","Dollars",
//	    "Boliviano", "Pula", "Real", "Lev","Dollars","Franc","Pesos","Yuan Renmimbi",
//	    "Dollars","Pesos","Kuna","Pesos","Pounds","Koruna","Kroner","Pesos",
//	    "Dollars","Sucre","Pounds","Colon","Kroon","Birr","Pound","Krone","Dollars",
//	    "Markka","Franc","Pound","Drachmas","Dollars","Dollars","Forint","Krona",
//	    "Rupees","Rupiah","Rial","Dinar","Punt","Shekels","Dollars","Dinar","Tenge",
//	    "Dinar","Pounds","Francs","Ringgit","Pesos","Rupees","Dollars","Kroner",
//	    "Rupees","Pesos","Zloty","Escudo","Leu","Rubles","Riyal","Dollars","Koruna",
//	    "Rand","Won","Pesetas","Dinar","Krona","Dollars","Baht","Dollars","Lira",
//	    "Bolivar","Kwacha"
//	};
//
//        if(numCountries>countriesNames.length){
//            numCountries=countriesNames.length-1;
//        }
//
//	for(int i = 0; i < numCountries ; i++){
//
//            //Country name = key
//            insert(exchanges[i], countriesNames[i],"Countries" ,"CO_EXCHANGE" , writeConsitency);
//            insert(currencies[i], countriesNames[i],"Countries" , "CO_CURRENCY", writeConsitency);
//	    Country country = new Country(countriesNames[i], exchanges[i], currencies[i]);
//            this.countries.add(country);
//	}
//    }
//
//
//
//    public void des_populate(int writeQuorum, int readQuorum) {
//    }
//
//    public static byte[] getBytes(Object obj) {
//        try {
//            ByteArrayOutputStream bos = new ByteArrayOutputStream();
//            ObjectOutputStream oos = new ObjectOutputStream(bos);
//            oos.writeObject(obj);
//            oos.flush();
//            oos.close();
//            bos.close();
//            byte[] data = bos.toByteArray();
//            return data;
//        } catch (IOException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        return null;
//    }
//
//    public static Object toObject(byte[] bytes){
//        try {
//            Object object = null;
//            object = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes)).readObject();
//            return object;
//        } catch (IOException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (ClassNotFoundException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        return null;
//}
//
//
//    public void ShowValues(String column_family) {
//        try {
//            SlicePredicate predicate = new SlicePredicate();
//            SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, 300);
//
//            ColumnParent parent = new ColumnParent(column_family, null);
//            predicate.setSlice_range(range);
//
//            List<String> keys = client.get_key_range(Keyspace, column_family, "", "", 300, ConsistencyLevel.ONE);
//            Map<String, List<ColumnOrSuperColumn>> results = client.multiget_slice(Keyspace, keys, parent, predicate, ConsistencyLevel.ONE);
//            for (String key : results.keySet()) {
//                List<ColumnOrSuperColumn> line = results.get(key);
//                StringBuilder builder = new StringBuilder();
//                for (ColumnOrSuperColumn column : line) {
//                    Column c = column.getColumn();
//                    builder.append(" Name: " + new String(c.getName()) + " Value: " + new String(c.getValue()) + "||");
//                }
//                System.out.println("Key on column: " + key + "with values: " + builder.toString());
//            }
//
//        } catch (InvalidRequestException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (UnavailableException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TException ex) {
//            Logger.getLogger(PopulateDatabase.class.getName()).log(Level.SEVERE, null, ex);
//        }
//    }
//
//    public void info() {
//
//        float insert_average =  (float) ((simple_time * 0.1) / (simple_inserts * 0.1));
//        float super_insert_average = (float) ((super_time * 0.1) / (super_inserts * 0.1));
//
//        System.out.println("Average time on insert in a column family: "+insert_average);
//        System.out.println("Average time on insert in a Super column family: "+super_insert_average);
//
//
////        Namegenerator ng = new Namegenerator("src/JaNaG_Source/languages.txt", "src/JaNaG_Source/semantics.txt");
////        //String[] ns  =  ng.getPatterns();
////        String[] ns = ng.getGenders("Orkisch");
////        for (String ss : ns) {
////            System.out.println(ss);
////
////        }
//
//        /**
//        Patterns:
//
//        Pseudo-Altdeutsch
//        Menschlich Fantasy (Mann , Frau)
//        Götternamen
//        Dämonen
//        Griechisch
//        Spanisch
//        Italienisch
//        Irisch
//        Französisch
//        Polnisch
//        Hebräisch (Mann , Frau)
//        Orkisch
//        US-Zensus//english
//         *
//         */
////
////        try {
////            Map<String, Map<String, String>> info = client.describe_keyspace("Tpcw");
////            System.out.println("Info =" + info.toString());
////
////        } catch (NotFoundException ex) {
////            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
////        } catch (TException ex) {
////            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
////        }
//
//
//    }
//
//
//        private static GregorianCalendar getRandomDate(int firstYar, int lastYear) {
//	int month, day, year, maxday;
//
//	year = getRandomInt(firstYar, lastYear);
//	month = getRandomInt(0,11);
//
//	maxday = 31;
//	if (month == 3 | month ==5 | month == 8 | month == 10)
//	    maxday = 30;
//	else if (month == 1)
//	    maxday = 28;
//
//	day = getRandomInt(1, maxday);
//	return new GregorianCalendar(year, month, day);
//    }
//
//
//       private static int getRandomInt(int lower, int upper){
//
//	int num = (int) Math.floor(rand.nextDouble()*((upper+1)-lower));
//	if(num+lower > upper || num+lower < lower){
//	    System.out.println("ERROR: Random returned value of of range!");
//	    System.exit(1);
//	}
//	return num + lower;
//    }
//
//           private static int getRandomNString(int num_digits){
//	int return_num = 0;
//	for(int i = 0; i < num_digits; i++){
//	    return_num += getRandomInt(0, 9) *
//		(int) java.lang.Math.pow(10.0, (double) i);
//	}
//	return return_num;
//    }
//
//        private static String getRandomAString(int min, int max){
//	String newstring = new String();
//	int i;
//	final char[] chars = {'a','b','c','d','e','f','g','h','i','j','k',
//			      'l','m','n','o','p','q','r','s','t','u','v',
//			      'w','x','y','z','A','B','C','D','E','F','G',
//			      'H','I','J','K','L','M','N','O','P','Q','R',
//			      'S','T','U','V','W','X','Y','Z','!','@','#',
//			      '$','%','^','&','*','(',')','_','-','=','+',
//			      '{','}','[',']','|',':',';',',','.','?','/',
//			      '~',' '}; //79 characters
//	int strlen = (int) Math.floor(rand.nextDouble()*((max-min)+1));
//	strlen += min;
//	for(i = 0; i < strlen; i++){
//	    char c = chars[(int) Math.floor(rand.nextDouble()*79)];
//	    newstring = newstring.concat(String.valueOf(c));
//	}
//	return newstring;
//    }
//}