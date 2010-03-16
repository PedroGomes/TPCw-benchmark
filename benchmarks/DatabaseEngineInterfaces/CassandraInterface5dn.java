///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package benchmarks.DatabaseEngineInterfaces;
//
//import benchmarks.helpers.JsonUtil;
//
//import benchmarks.helpers.BenchmarkUtil;
//import benchmarks.interfaces.CRUD;
//import benchmarks.interfaces.Entity;
//import java.io.BufferedReader;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.Random;
//import java.util.TreeMap;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//import org.apache.cassandra.service.Cassandra.Client;
//import org.apache.cassandra.service.Column;
//import org.apache.cassandra.service.ColumnOrSuperColumn;
//import org.apache.cassandra.service.ColumnParent;
//import org.apache.cassandra.service.ColumnPath;
//import org.apache.cassandra.service.ConsistencyLevel;
//import org.apache.cassandra.service.InvalidRequestException;
//import org.apache.cassandra.service.KeySlice;
//import org.apache.cassandra.service.NotFoundException;
//import org.apache.cassandra.service.SlicePredicate;
//import org.apache.cassandra.service.SliceRange;
//import org.apache.cassandra.service.TimedOutException;
//import org.apache.cassandra.service.UnavailableException;
//
//
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
//public class CassandraInterface5dn implements CRUD, benchmarks.TpcwBenchmark.TPCWBenchmarkInterface {
//
//    public String Keyspace = "Tpcw";
//    int search_slice_ratio = 1000;
//   // static Client client;
//    static ArrayList<Client> clients;
//    Map<String, String> paths;
//    Map<String, ConsistencyLevel> consistencyLevels;
//    ConsistencyLevel DEFAULT = ConsistencyLevel.QUORUM;
//    TreeMap<String, TreeMap<String, String>> keyMap;
//    public static ConsistencyLevel INSERT_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
//    public static ConsistencyLevel REMOVE_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
//    public static ConsistencyLevel RANGE_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
//    public static ConsistencyLevel TRANSACTIONAL_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
//    public static ConsistencyLevel READ_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
//    public static ConsistencyLevel WRITE_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
//
//    public CassandraInterface5dn() throws IOException {
//
//
//        paths = new TreeMap<String, String>();
//        consistencyLevels = new TreeMap<String, ConsistencyLevel>();
//        keyMap = new TreeMap<String, TreeMap<String, String>>();
//
//        FileInputStream in = null;
//        String jsonString_r = "";
//        try {
//
//            in = new FileInputStream("CassandraDB.json");
//            BufferedReader bin = new BufferedReader(new InputStreamReader(in));
//            String s = "";
//            StringBuilder sb = new StringBuilder();
//            while (s != null) {
//                sb.append(s);
//                s = bin.readLine();
//            }
//
//            jsonString_r = sb.toString().replace("\n", "");
//            bin.close();
//            in.close();
//
//        } catch (FileNotFoundException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } finally {
//            try {
//                in.close();
//            } catch (IOException ex) {
//                Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//            }
//        }
//
//        Map<String, Map<String, String>> map = JsonUtil.getMapMapFromJsonString(jsonString_r);
//
//        if (!map.containsKey("ColumnPaths")) {
//            System.out.println("WARNING: COLUMN PATHS NOT FOUND");
//        } else {
//            paths = (Map<String, String>) map.get("ColumnPaths");
//        }
//        if (!map.containsKey("ConsistencyLevels")) {
//            System.out.println("WARNING: CONSITENCY LEVELS NOT FOUND, QUORUM ASSUMED");
//        } else {
//            Map<String, String> CL = (Map<String, String>) map.get("ConsistencyLevels");
//            INSERT_CONSISTENCY_LEVEL = ConsistencyLevel.valueOf(CL.get("INSERT_CONSISTENCY_LEVEL"));
//            REMOVE_CONSISTENCY_LEVEL = ConsistencyLevel.valueOf(CL.get("REMOVE_CONSISTENCY_LEVEL"));
//            RANGE_CONSISTENCY_LEVEL = ConsistencyLevel.valueOf(CL.get("RANGE_CONSISTENCY_LEVEL"));
//            TRANSACTIONAL_CONSISTENCY_LEVEL = ConsistencyLevel.valueOf(CL.get("TRANSACTIONAL_CONSISTENCY_LEVEL"));
//            READ_CONSISTENCY_LEVEL = ConsistencyLevel.valueOf(CL.get("READ_CONSISTENCY_LEVEL"));
//            WRITE_CONSISTENCY_LEVEL = ConsistencyLevel.valueOf(CL.get("WRITE_CONSISTENCY_LEVEL"));
//        }
//
//
//        Keyspace = "Tpcw";
//
//        if (!map.containsKey("DataBaseInfo")) {
//            System.out.println("ERROR: NO DATABASE INFO FOUND DEFAULTS ASSUMED: KEYSPACE=Tpcw");
//        } else {
//            Map<String, String> CI = (Map<String, String>) map.get("DataBaseInfo");
//            Keyspace = CI.get("keyspace");
//        }
//
//        TreeMap<String, Integer> connections = new TreeMap<String, Integer>();
//
//
//        if (!map.containsKey("DataBaseConnections")) {
//            System.out.println("ERROR: NO CONNECTION INFO FOUND DEFAULTS ASSUMED: [HOST=localhost, PORT=9160] ");
//            connections.put("localhost", 9160);
//        } else {
//            Map<String, String> CI = (Map<String, String>) map.get("DataBaseConnections");
//            for (String host : CI.keySet()) {
//                int port = Integer.parseInt(CI.get(host).trim());
//                connections.put(host, port);
//            }
//        }
//        if (connections.isEmpty()) {
//            System.out.println("ERROR: NO CONNECTION INFO FOUND DEFAULTS ASSUMED: [HOST=localhost, PORT=9160] ");
//            connections.put("localhost", 9160);
//        }
//
//        for (String host : connections.keySet()) {
//            try {
//                TSocket socket = new TSocket(host, connections.get(host));
//                TProtocol prot = new TBinaryProtocol(socket);
//                Client c = new Client(prot);
//                socket.open();
//                clients.add(c);
//            } catch (TTransportException ex) {
//                Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//            }
//        }
//    }
//
//
//    public Client getClient(){
//        Random r =  new Random();
//        Client c = clients.get(r.nextInt(clients.size()));
//        return c;
//    }
//
//    /*******************
//     * CRUD OPERATIONS *
//     ******************/
//
//    public void insert(String key, String path, Entity value) {
//
//        TreeMap<String, Object> fieldsToInsert = new TreeMap<String, Object>();
//        fieldsToInsert = value.getValuesToInsert();
//
//        if (paths.containsKey(path)) {
//            String column = path;
//            String superColumnParameter = paths.get(path);
//            String ColumnFamilyKey = (String) fieldsToInsert.get(superColumnParameter);
//            insertkey(path, ColumnFamilyKey, key);
//
//            for (String valueName : fieldsToInsert.keySet()) {
//                insertInSuperColumn(fieldsToInsert.get(valueName), ColumnFamilyKey, column, key, valueName, DEFAULT);
//            }
//        } else {
//            for (String valueName : fieldsToInsert.keySet()) {
//                insert(fieldsToInsert.get(valueName), key, path, valueName, DEFAULT);
//            }
//        }
//
//    }
//
//    public void update(String key, String path, String column, Object value) {
//
//
//        if (paths.containsKey(path)) {
//            String ColumnFamily = path;
//            String ColumnFamilyKey = keyMap.get(ColumnFamily).get(key);
//
//            insertInSuperColumn(value, ColumnFamilyKey, path, key, column, DEFAULT);
//
//        } else {
//            insert(value, key, path, column, DEFAULT);
//        }
//
//    }
//
//    public Object read(String key, String path, String column) {
//
//
//        if (paths.containsKey(path)) {
//            String columnFamily = path;
//            String ColumnFamilyKey = keyMap.get(path).get(key);//what is the key for the supercolumnSet
//
//            readfromSuperColumn(ColumnFamilyKey, columnFamily, column, key, DEFAULT);
//
//        } else {
//            readfromColumn(key, path, column, DEFAULT);
//
//        }
//        return null;
//    }
//
//    /**DATA BASE METHODS**/
//    public void insert(Object value, String key, String column_family, String column, ConsistencyLevel writeConsistency) {
//        try {
//            //ColumnPath path = new ColumnPath(column_family, null, null);
//            ColumnPath path = new ColumnPath(column_family);
//            path.setColumn(column.getBytes());
//            byte[] valueBytes = BenchmarkUtil.getBytes(value);
//            long time = System.currentTimeMillis();
//
//            getClient().insert(Keyspace, key, path, valueBytes, System.currentTimeMillis(), writeConsistency);
//
//            time = System.currentTimeMillis() - time;
//            // simple_time += time;
//            // simple_inserts++;
//            // System.out.println("Time to insert: " + time);
//        } catch (TimedOutException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (InvalidRequestException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (UnavailableException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        }
//    }
//
//    public void insertInSuperColumn(Object value, String key, String column_family, String SuperColumn, String Column, ConsistencyLevel writeConsistency) {
//        try {
//            //ColumnPath path = new ColumnPath(column_family, SuperColumn.getBytes(), Column.getBytes());
//            ColumnPath path = new ColumnPath(column_family);
//            path.setSuper_column(SuperColumn.getBytes());
//            path.setColumn(Column.getBytes());
//
//
//
//            byte[] valueBytes = BenchmarkUtil.getBytes(value);
//            long time = System.currentTimeMillis();
//            getClient().insert(Keyspace, key, path, valueBytes, System.currentTimeMillis(), writeConsistency);
//            time = System.currentTimeMillis() - time;
//
//            //System.out.println("Time to insert: " + time);
//        } catch (TimedOutException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (InvalidRequestException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (UnavailableException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        }
//
//    }
//
//    public Object readfromColumn(String key, String ColumnFamily, String column, ConsistencyLevel con) {
//        try {
//            ColumnPath path = new ColumnPath(ColumnFamily);
//            path.setColumn(column.getBytes());
//            byte[] value = getClient().get(Keyspace, key, path, con).column.value;
//            Object o = BenchmarkUtil.toObject(value);
//            return o;
//
//        } catch (InvalidRequestException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (NotFoundException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (UnavailableException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TimedOutException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        return null;
//    }
//
//    public Object readfromSuperColumn(String familykey, String columnFamily, String column, String superColumnKey, ConsistencyLevel con) {
//
//        try {
//            ColumnPath path = new ColumnPath(columnFamily);
//            path.setSuper_column(superColumnKey.getBytes());
//            path.setColumn(column.getBytes());
//            byte[] value = getClient().get(Keyspace, familykey, path, con).column.value;
//            Object o;
//            o = BenchmarkUtil.toObject(value);
//            return o;
//
//        } catch (InvalidRequestException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (NotFoundException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (UnavailableException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TimedOutException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        return null;
//    }
//
//    /**
//     * @param ColumnKey The column family key;
//     * @param SuperKey the super column key;
//     * The super column key will identify the Column FAmily key to lookup;
//     *
//     **/
//    public void insertkey(String path, String ColumnKey, String SuperKey) {
//        if (!keyMap.containsKey(path)) {
//            keyMap.put(path, new TreeMap<String, String>());
//        }
//        keyMap.get(path).put(SuperKey, ColumnKey);
//
//    }
//
//    public void remove(String key, String column) {
//        try {
//            //ColumnPath path = new ColumnPath(column, null, null);
//            ColumnPath path = new ColumnPath(column);
//            getClient().remove(Keyspace, key, path, System.currentTimeMillis(), DEFAULT);
//        } catch (TimedOutException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (InvalidRequestException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (UnavailableException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        }
//    }
//
//
//    public void truncate(String column_family, int number_keys) {
//        System.out.println("Removing ColumnFamily:" + column_family + "Number of keys: " + number_keys);
//        try {
//
//            SlicePredicate predicate = new SlicePredicate();
//            SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, 300);
//            ColumnParent parent = new ColumnParent(column_family);
//            predicate.setSlice_range(range);
//
//            String last_key = "";
//
//            float f = number_keys / search_slice_ratio;
//            int cycles = (int) Math.floor(f) + 1;
//            System.out.println("Cycles: " + cycles);
//
//            for (int cy = 0; cy < cycles; cy++) {
//                int i = 0;
//                List<KeySlice> keys = getClient().get_range_slice(Keyspace, parent, predicate, last_key, "", search_slice_ratio, ConsistencyLevel.ONE);
//
//                if (keys.isEmpty()) {
//                    System.out.println("The key range is empty");
//                } else {
//                    last_key = keys.get(keys.size() - 1).key;
//                }
//                // Map<String, List<ColumnOrSuperColumn>> results = getClient().multiget_slice(Keyspace, keys, parent, predicate, ConsistencyLevel.ONE);
//                ColumnPath path = new ColumnPath(column_family);
//                for (KeySlice key : keys) {
//                    if (!key.columns.isEmpty()) {
//                        getClient().remove(Keyspace, key.key, path, System.currentTimeMillis(), DEFAULT);
//                        i++;
//                    }
//                }
//                System.out.println("cycle " + cy + " with " + i + " alive keys in " + keys.size());
//                if (keys.size() < search_slice_ratio) {
//                    break;
//                }
//            }
//
//
//
//
//        } catch (TimedOutException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (InvalidRequestException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (UnavailableException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        }
//    }
//
//    private List<ColumnOrSuperColumn> getAllColumnsfromCF(String path, String key) {
//
//        ColumnParent parent = new ColumnParent(path);
//        SlicePredicate predicate = new SlicePredicate();
//        SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, 300);
//        try {
//
//
//            List<ColumnOrSuperColumn> result = getClient().get_slice(Keyspace, key, parent, predicate, DEFAULT);
//            return result;
//        } catch (InvalidRequestException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (UnavailableException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TimedOutException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        return null;
//    }
//
//    private TreeMap<String, List<Column>> getAllColumnsfromSuperCF(String path, String key) {
//
//        ColumnParent parent = new ColumnParent(path);
//
//        SlicePredicate predicate = new SlicePredicate();
//        SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, 300);
//        try {
//
//
//            List<ColumnOrSuperColumn> supercolumns = getClient().get_slice(Keyspace, key, parent, predicate, DEFAULT);
//
//
//            TreeMap<String, List<Column>> result = new TreeMap<String, List<Column>>();
//            for (ColumnOrSuperColumn sc : supercolumns) {
//
//                List<Column> columns = sc.super_column.columns;
//
//                result.put(new String(sc.super_column.name), columns);
//
//            }
//            return result;
//
//        } catch (InvalidRequestException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (UnavailableException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TimedOutException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        return null;
//    }
//
//    /**************************************/
//    /****  TPCW benchmark operations  *****/
//    /**************************************/
//    public void searchTop10Books() {
//        try {
//            String column_family = "Authors";
//            SlicePredicate predicate = new SlicePredicate();
//            SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, 300);
//            ColumnParent parent = new ColumnParent(column_family);
//            predicate.setSlice_range(range);
//
//            String last_key = "";
//            boolean terminated = false;
//
//            while (!terminated) {
//                int i = 0;
//                List<KeySlice> keys = getClient().get_range_slice(Keyspace, parent, predicate, last_key, "", search_slice_ratio, ConsistencyLevel.ONE);
//
//                if (keys.isEmpty()) {
//                    System.out.println("INFO: The key range is empty");
//                } else {
//                    last_key = keys.get(keys.size() - 1).key;
//                }
//                // Map<String, List<ColumnOrSuperColumn>> results = client.multiget_slice(Keyspace, keys, parent, predicate, ConsistencyLevel.ONE);
//                ColumnPath path = new ColumnPath(column_family);
//                for (KeySlice key : keys) {
//
//                    List<ColumnOrSuperColumn> line = key.getColumns();
//
//
//                }
//                if (keys.size() < search_slice_ratio) {
//                    terminated = true;
//                }
//            }
//        } catch (TimedOutException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (InvalidRequestException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (UnavailableException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (TException ex) {
//            Logger.getLogger(CassandraInterface5dn.class.getName()).log(Level.SEVERE, null, ex);
//        }
//
//    }
//
//    public Map<String, Integer> readCart(int cart) {
//
//        TreeMap<String, List<Column>> columns = getAllColumnsfromSuperCF("ShoppingCart", cart + "");
//        Map<String, Integer> result = new TreeMap<String, Integer>();
//
//        for (String cartline : columns.keySet()) {
//
//            String book_id = cartline;
//            int qty = 0;
//
//            for (Column c : columns.get(cartline)) {
//                if (new String(c.name).equals("QTY")) {
//                    qty = (Integer) BenchmarkUtil.toObject(c.getValue());
//                }
//            }
//
//            result.put(book_id, qty);
//        }
//
//        return result;
//
//    }
//
//    public void addToCart(int cart, String item, int qty_to_add) {
//
//        Object o = readfromSuperColumn(cart + "", "ShoppingCart", "QTY", item, DEFAULT);
//        int qty = 0;
//        if (o != null) {
//            qty = (Integer) o;
//            qty += qty_to_add;
//        }
//        insertInSuperColumn(qty_to_add, cart + "", "ShoppingCart", item, "QTY", DEFAULT);
//    }
//
//    public BuyingResult BuyCartItem(String item, int qty) {
//
//        Object o = readfromColumn(item, "Items", "I_STOCK", DEFAULT);
//        if (o == null) {
//            return BuyingResult.DOES_NOT_EXIST;
//        }
//        int stock = (Integer) o;
//        if (stock > 0) {
//            stock -= qty;
//            insert(stock, item, "Items", "I_STOCK", DEFAULT);
//            Object new_o = readfromColumn(item, "Items", "I_STOCK", DEFAULT);
//            if (new_o == null) {
//                return BuyingResult.DOES_NOT_EXIST;
//            }
//            int result = (Integer) new_o;
//            if (result < 0) {
//                return BuyingResult.OUT_OF_STOCK;
//            }
//        } else {
//            return BuyingResult.NOT_AVAILABLE;
//        }
//        return BuyingResult.BOUGHT;
//    }
//}