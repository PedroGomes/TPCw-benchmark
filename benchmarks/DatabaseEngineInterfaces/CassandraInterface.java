/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package benchmarks.DatabaseEngineInterfaces;

import benchmarks.helpers.JsonUtil;

import benchmarks.helpers.BenchmarkUtil;
import benchmarks.interfaces.DataBaseCRUDInterface;
import benchmarks.interfaces.Entity;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.cassandra.service.*;
import org.apache.cassandra.service.Cassandra.Client;


import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

/**
 * @author pedro
 */
public class CassandraInterface implements DataBaseCRUDInterface, benchmarks.TpcwBenchmark.TPCWBenchmarkInterface {

    private String Keyspace = "Tpcw";
    private long simulatedDelay = 0;
    private int search_slice_ratio = 1000;


    private TreeMap<String, Integer> connections = new TreeMap<String, Integer>();
    private Map<String, String> paths;
    private ConcurrentMap<String, TreeMap<String, String>> keyMap;
    private TreeMap<String, Integer> consistencyLevels;
    //Map<String, ConsistencyLevel> consistencyLevels;

    public static int INSERT_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
    public static int REMOVE_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
    public static int RANGE_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
    public static int TRANSACTIONAL_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
    public static int READ_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
    public static int WRITE_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

    int r;


    public CassandraInterface() throws IOException {

        paths = new TreeMap<String, String>();
        consistencyLevels = new TreeMap<String, Integer>();

        consistencyLevels.put("ZERO", 0);
        consistencyLevels.put("ONE", 1);
        consistencyLevels.put("QUORUM", 2);
        consistencyLevels.put("DCQUORUM", 3);
        consistencyLevels.put("DCQUORUMSYNC", 4);
        consistencyLevels.put("ALL", 5);

        keyMap = new ConcurrentHashMap<String, TreeMap<String, String>>();

        FileInputStream in = null;
        String jsonString_r = "";
        try {

            in = new FileInputStream("CassandraDB.json");
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
        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        Map<String, Map<String, String>> map = JsonUtil.getMapMapFromJsonString(jsonString_r);

        if (!map.containsKey("ColumnPaths")) {
            System.out.println("WARNING: COLUMN PATHS NOT FOUND");
        } else {
            paths = (Map<String, String>) map.get("ColumnPaths");
        }
        if (!map.containsKey("ConsistencyLevels")) {
            System.out.println("WARNING: CONSITENCY LEVELS NOT FOUND, QUORUM ASSUMED");
        } else {
            Map<String, String> CL = (Map<String, String>) map.get("ConsistencyLevels");


            INSERT_CONSISTENCY_LEVEL = consistencyLevels.get(CL.get("INSERT_CONSISTENCY_LEVEL"));
            REMOVE_CONSISTENCY_LEVEL = consistencyLevels.get(CL.get("REMOVE_CONSISTENCY_LEVEL"));
            RANGE_CONSISTENCY_LEVEL = consistencyLevels.get(CL.get("RANGE_CONSISTENCY_LEVEL"));
            TRANSACTIONAL_CONSISTENCY_LEVEL = consistencyLevels.get(CL.get("TRANSACTIONAL_CONSISTENCY_LEVEL"));
            READ_CONSISTENCY_LEVEL = consistencyLevels.get(CL.get("READ_CONSISTENCY_LEVEL"));
            WRITE_CONSISTENCY_LEVEL = consistencyLevels.get(CL.get("WRITE_CONSISTENCY_LEVEL"));
        }


        Keyspace = "Tpcw";

        if (!map.containsKey("DataBaseInfo")) {
            System.out.println("ERROR: NO DATABASE INFO FOUND DEFAULTS ASSUMED: KEYSPACE=Tpcw");
        } else {
            Map<String, String> CI = (Map<String, String>) map.get("DataBaseInfo");
            Keyspace = CI.get("keyspace");
        }


        if (!map.containsKey("DataBaseConnections")) {
            System.out.println("ERROR: NO CONNECTION INFO FOUND DEFAULTS ASSUMED: [HOST=localhost, PORT=9160] ");
            connections.put("localhost", 9160);
        } else {
            Map<String, String> CI = (Map<String, String>) map.get("DataBaseConnections");
            for (String host : CI.keySet()) {
                int port = Integer.parseInt(CI.get(host).trim());
                connections.put(host, port);
            }
        }
        if (connections.isEmpty()) {
            System.out.println("ERROR: NO CONNECTION INFO FOUND DEFAULTS ASSUMED: [HOST=localhost, PORT=9160] ");
            connections.put("localhost", 9160);
        }
    }

    public CRUDclient getClient() {
        return new CassandraClient(paths, connections);
    }

    public void simulatedDelay(long delay) {
        simulatedDelay = delay;
    }

    public void delay() {
        try {
            Thread.sleep(simulatedDelay);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    public BenchmarkInterfaceClient getBenchmarkClient() {
        return new CassandraClient(paths, connections);
    }

    class CassandraClient implements CRUDclient, TPCWBenchmarkInterfaceClient {

        TreeMap<String, Integer> connections = new TreeMap<String, Integer>();
        private ArrayList<Client> clients;
        Map<String, String> paths;
        int last = 0;

        CassandraClient(Map<String, String> paths, TreeMap<String, Integer> connections) {
            this.paths = paths;
            this.connections = connections;
            clients = new ArrayList<Client>();

            Map<String, Integer> sortedConnections = BenchmarkUtil.randomizeMap(connections);

            for (String rand_host : sortedConnections.keySet()) {
                String host = rand_host.split(":")[1];
                //System.out.println("Connecting to: " + host);
                try {
                    TSocket socket = new TSocket(host, connections.get(host));
                    TProtocol prot = new TBinaryProtocol(socket);
                    Client c = new Client(prot);
                    socket.open();
                    clients.add(c);
                } catch (TTransportException ex) {
                    System.out.println("[ERROR] FAILED TO CONNECT TO:" + host + ":port:" + connections.get(host) + " -> CLIENT IGNORED ");
                    //Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
                }

            }


        }

        public Client getCassandraClient() {

            boolean openClient = false;
            Client cl = null;

            while (!openClient) {   //until there is one open

                if (!clients.isEmpty()) {   //if none, then null...
                    cl = clients.get(last);
                    if (!cl.getInputProtocol().getTransport().isOpen()) {
                        clients.remove(last);
                    } else {
                        openClient = true;
                    }
                    last++;
                    last = last >= clients.size() ? 0 : last;

                } else {
                    openClient = true;
                }
            }
            return cl;

        }

        public void closeClient() {
            for (Client c : clients) {
                c.getInputProtocol().getTransport().close();
                c = null;
            }
            clients = null;
            connections = null;
            paths = null;
            System.gc();
        }

        /**
         * *****************
         * DataBaseCRUDInterface OPERATIONS *
         * *****************
         */



        public Object insert(String key, String path, Entity value) {

            TreeMap<String, Object> fieldsToInsert;
            fieldsToInsert = value.getValuesToInsert();

            String ColumnFamilyKey = "";

            ArrayList<ColumnOrSuperColumn> columns = new ArrayList<ColumnOrSuperColumn>();

            if (paths.containsKey(path)) {

                String superColumnParameter = paths.get(path);
                ColumnFamilyKey = (fieldsToInsert.get(superColumnParameter) + "");
                insertkey(path, ColumnFamilyKey, key);

                List<Column> SC_columns = new ArrayList<Column>();
                for (String valueName : fieldsToInsert.keySet()) {
                    Column column_to_insert = null;

                        column_to_insert = new Column(valueName.getBytes(), BenchmarkUtil.getBytes(fieldsToInsert.get(valueName)), System.currentTimeMillis());

                    SC_columns.add(column_to_insert);
                    // insertInSuperColumn(fieldsToInsert.get(valueName), ColumnFamilyKey, column, key, valueName, INSERT_CONSISTENCY_LEVEL);
                }
                SuperColumn value_superColumn = null;
                try {
         

                    value_superColumn = new SuperColumn(key.getBytes("UTF-8"), SC_columns);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
                ColumnOrSuperColumn column_or_superColumn = new ColumnOrSuperColumn(null, value_superColumn);
                columns.add(column_or_superColumn);

            } else {

                ColumnFamilyKey = key;
                for (String valueName : fieldsToInsert.keySet()) {
                    Column column = new Column(valueName.getBytes(), BenchmarkUtil.getBytes(fieldsToInsert.get(valueName)), System.currentTimeMillis());
                    ColumnOrSuperColumn column_or_superColumn = new ColumnOrSuperColumn(column, null);
                    columns.add(column_or_superColumn);
                    // insert(fieldsToInsert.get(valueName), key, path, valueName, INSERT_CONSISTENCY_LEVEL);
                }
            }

            batch_insert(ColumnFamilyKey, path, columns);
            delay();


            return null;
        }


        public void update(String key, String path, String column, Object value) {


            if (paths.containsKey(path)) {
                String ColumnFamily = path;
                String ColumnFamilyKey = keyMap.get(ColumnFamily).get(key);
                insertInSuperColumn(value, ColumnFamilyKey, path, key, column, WRITE_CONSISTENCY_LEVEL);

            } else {
                insert(value, key, path, column, WRITE_CONSISTENCY_LEVEL);
            }

        }

        public Object read(String key, String path, String column) {

            Object result = null;
            if (paths.containsKey(path)) {
                String columnFamily = path;
                String ColumnFamilyKey = keyMap.get(path).get(key);//what is the key for the supercolumnSet

                result = readfromSuperColumn(ColumnFamilyKey, columnFamily, column, key, READ_CONSISTENCY_LEVEL);

            } else {
                result = readfromColumn(key, path, column, READ_CONSISTENCY_LEVEL);

            }
            return result;
        }

        public Map<String, Map<String, Object>> rangeQuery(String column_family, List<String> fields, int limit) {


            Map<String, Map<String, Object>> results = new TreeMap<String, Map<String, Object>>();
            try {
                SlicePredicate predicate = new SlicePredicate();
                SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, 300);

                ColumnParent parent = new ColumnParent();
                parent.setColumn_family(column_family);
                predicate.setSlice_range(range);

                String last_key = "";

                boolean terminated = false;
                limit = (limit < 0) ? -1 : limit;
                int number_keys = 0;

                while (!terminated) {
                    List<KeySlice> keys = getCassandraClient().get_range_slice(Keyspace, parent, predicate, last_key, "", search_slice_ratio, RANGE_CONSISTENCY_LEVEL);
                    delay();

                    if (keys.isEmpty()) {
                        System.out.println("The key range is empty");
                    } else {
                        last_key = keys.get(keys.size() - 1).key;
                    }
                    // Map<String, List<ColumnOrSuperColumn>> results = getCassandraClient().multiget_slice(Keyspace, keys, parent, predicate, ConsistencyLevel.ONE);
                    ColumnPath path = new ColumnPath();
                    path.setColumn_family(column_family);
                    for (KeySlice key : keys) {
                        if (!key.columns.isEmpty()) {
                            for (ColumnOrSuperColumn c : key.getColumns()) {
                                if (c.isSetSuper_column()) {
                                    for (Column co : c.getSuper_column().columns) {
                                        if (fields.contains(new String(co.getName()))) {
                                            if (!results.containsKey(key.key)) {
                                                Map<String, Object> returned_fields = new TreeMap<String, Object>();
                                                returned_fields.put(new String(co.getName()), BenchmarkUtil.toObject(co.getValue()));
                                                results.put(key.key, returned_fields);
                                            } else {
                                                results.get(key.key).put(new String(co.getName()), BenchmarkUtil.toObject(co.getValue()));
                                            }
                                        }
                                    }
                                } else {
                                    if (fields.contains(new String(c.getColumn().getName())))

                                        if (!results.containsKey(key.key)) {
                                            Map<String, Object> returned_fields = new TreeMap<String, Object>();
                                            returned_fields.put(new String(c.getColumn().getName()), BenchmarkUtil.toObject(c.getColumn().getValue()));
                                            results.put(key.key, returned_fields);
                                        } else {
                                            results.get(key.key).put(new String(c.getColumn().getName()), BenchmarkUtil.toObject(c.getColumn().getValue()));
                                        }
                                }
                            }
                        }
                        number_keys++;
                        if (number_keys >= limit && limit != -1) {
                            terminated = true;
                        }
                    }
                    if (keys.size() < search_slice_ratio) {
                        terminated = true;
                    }
                }


            } catch (TimedOutException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InvalidRequestException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (UnavailableException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            }

            return results;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public void remove(String key, String columnFamily, String column) {
            try {
                //ColumnPath path = new ColumnPath(column, null, null);
                ColumnPath path = new ColumnPath();
                path.setColumn_family(columnFamily);
                path.setColumn(column.getBytes());

                getCassandraClient().remove(Keyspace, key, path, System.currentTimeMillis(), REMOVE_CONSISTENCY_LEVEL);
                delay();

            } catch (TimedOutException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InvalidRequestException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (UnavailableException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        public void truncate(String column_family) {

            System.out.println("Removing ColumnFamily: " + column_family);
            try {

                SlicePredicate predicate = new SlicePredicate();
                SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, 3000);

                ColumnParent parent = new ColumnParent();
                parent.setColumn_family(column_family);
                predicate.setSlice_range(range);

                String last_key = "";
                int alive_keys = 0;
                int total_keys = 0;
                boolean terminated = false;

                while (!terminated) {
                    Client c = getCassandraClient();
                    List<KeySlice> keys = c.get_range_slice(Keyspace, parent, predicate, last_key, "", search_slice_ratio, RANGE_CONSISTENCY_LEVEL);
                    delay();

                    if (keys.isEmpty()) {
                        System.out.println("The key range is empty");
                    } else {
                        last_key = keys.get(keys.size() - 1).key;
                    }
                    // Map<String, List<ColumnOrSuperColumn>> results = getCassandraClient().multiget_slice(Keyspace, keys, parent, predicate, ConsistencyLevel.ONE);
                    ColumnPath path = new ColumnPath();
                    path.setColumn_family(column_family);
                    for (KeySlice key : keys) {


                        if (!key.columns.isEmpty()) {
                            getCassandraClient().remove(Keyspace, key.key, path, System.currentTimeMillis(), REMOVE_CONSISTENCY_LEVEL);
                            alive_keys++;
                        }
                        total_keys++;
                    }
                    if (keys.size() < search_slice_ratio) {
                        terminated = true;
                    }
                }
                System.out.println("[Column family " + column_family + "] total keys:" + total_keys + " alive keys:" + alive_keys);
            } catch (TimedOutException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InvalidRequestException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (UnavailableException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        /**
         * ******************
         * DATA BASE METHODS *
         * *******************
         */

        public void batch_insert(String key, String columnFamily, List<ColumnOrSuperColumn> mutation_columns) {
            TreeMap<String, List<ColumnOrSuperColumn>> mutations = new TreeMap<String, List<ColumnOrSuperColumn>>();
            mutations.put(columnFamily, mutation_columns);
            try {
                getCassandraClient().batch_insert(Keyspace, key, mutations, INSERT_CONSISTENCY_LEVEL);
            } catch (InvalidRequestException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } catch (UnavailableException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } catch (TimedOutException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } catch (TException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        public void insert(Object value, String key, String column_family, String column, int writeConsistency) {
            try {

                ColumnPath path = new ColumnPath();
                path.setColumn_family(column_family);
                path.setColumn(column.getBytes());
                byte[] valueBytes = BenchmarkUtil.getBytes(value);

                getCassandraClient().insert(Keyspace, key, path, valueBytes, System.currentTimeMillis(), writeConsistency);
                // simple_time += time;  s
                // simple_inserts++;
                // System.out.println("Time to insert: " + time);
            } catch (InvalidRequestException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (UnavailableException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TimedOutException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            delay();
        }

        public void insertInSuperColumn(Object value, String key, String column_family, String SuperColumn, String Column, int writeConsistency) {
            try {
                ColumnPath path = new ColumnPath(column_family, SuperColumn.getBytes(), Column.getBytes());
                //ColumnPath path = new ColumnPath(column_family);
                // path.setSuper_column(SuperColumn.getBytes());
                // path.setColumn(Column.getBytes());

                byte[] valueBytes = BenchmarkUtil.getBytes(value);
                getCassandraClient().insert(Keyspace, key, path, valueBytes, System.currentTimeMillis(), writeConsistency);

            } catch (TimedOutException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InvalidRequestException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (UnavailableException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            }
            delay();
        }

        public Object readfromColumn(String key, String ColumnFamily, String column, int con) {
            try {
                ColumnPath path = new ColumnPath();
                path.setColumn_family(ColumnFamily);
                path.setColumn(column.getBytes());
                byte[] value = getCassandraClient().get(Keyspace, key, path, con).column.value;
                Object o = BenchmarkUtil.toObject(value);
                delay();
                return o;

            } catch (InvalidRequestException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (NotFoundException ex) {
                return null;
            } catch (UnavailableException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TimedOutException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            }

            return null;
        }

        public Object readfromSuperColumn(String familykey, String columnFamily, String column, String superColumnKey, int con) {

            try {

                ColumnPath path = new ColumnPath(columnFamily, superColumnKey.getBytes(), column.getBytes());
//            ColumnPath path = new ColumnPath(columnFamily);
//            path.setSuper_column(superColumnKey.getBytes());
//            path.setColumn(column.getBytes());
                byte[] value = getCassandraClient().get(Keyspace, familykey, path, con).column.value;
                Object o;
                o = BenchmarkUtil.toObject(value);
                delay();
                return o;

            } catch (InvalidRequestException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (NotFoundException ex) {
                return null;
            } catch (UnavailableException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TimedOutException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            }
            return null;
        }


        /**
         * @param ColumnKey The column family key;
         * @param SuperKey  the super column key;
         *                  The super column key will identify the Column FAmily key to lookup;
         */
        public void insertkey(String path, String ColumnKey, String SuperKey) {

            if (path.equalsIgnoreCase("ShoppingCart")) {
                if (!keyMap.containsKey(path)) {
                    keyMap.put(path, new TreeMap<String, String>());
                }
                keyMap.get(path).put(SuperKey, ColumnKey);
            }
        }

        private List<ColumnOrSuperColumn> getAllColumnsfromCF(String path, String key, int consistencyLevel) {

            ColumnParent parent = new ColumnParent();
            parent.setColumn_family(path);

            SlicePredicate predicate = new SlicePredicate();
            SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, 300);
            try {
                List<ColumnOrSuperColumn> result = getCassandraClient().get_slice(Keyspace, key, parent, predicate, consistencyLevel);
                delay();
                return result;
            } catch (InvalidRequestException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (UnavailableException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TimedOutException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            }
            return null;
        }

        private TreeMap<String, List<Column>> getAllColumnsFromSuperCF(String path, String key, int consistencyLevel) {

            ColumnParent parent = new ColumnParent();
            parent.setColumn_family(path);

            SlicePredicate predicate = new SlicePredicate();
            SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, 300);
            predicate.setSlice_range(range);
            try {


                List<ColumnOrSuperColumn> superColumns = getCassandraClient().get_slice(Keyspace, key, parent, predicate, consistencyLevel);


                TreeMap<String, List<Column>> result = new TreeMap<String, List<Column>>();
                for (ColumnOrSuperColumn sc : superColumns) {

                    List<Column> columns = sc.super_column.columns;

                    result.put(new String(sc.super_column.name), columns);

                }
                delay();
                return result;

            } catch (InvalidRequestException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (UnavailableException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TimedOutException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            }
            return null;
        }


        /**************************************/
        /****  TPCW benchmark operations  ****/
        /**
         * ** /
         * ********************************
         */


        public void searchTop10Books() {
            try {
                String column_family = "Authors";
                SlicePredicate predicate = new SlicePredicate();
                SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, 300);

                ColumnParent parent = new ColumnParent();
                parent.setColumn_family(column_family);


                predicate.setSlice_range(range);

                String last_key = "";
                boolean terminated = false;

                while (!terminated) {
                    int i = 0;
                    List<KeySlice> keys = getCassandraClient().get_range_slice(Keyspace, parent, predicate, last_key, "", search_slice_ratio, RANGE_CONSISTENCY_LEVEL);

                    if (keys.isEmpty()) {
                        System.out.println("INFO: The key range is empty");
                    } else {
                        last_key = keys.get(keys.size() - 1).key;
                    }
                    // Map<String, List<ColumnOrSuperColumn>> results = client.multiget_slice(Keyspace, keys, parent, predicate, ConsistencyLevel.ONE);
                    ColumnPath path = new ColumnPath();
                    path.setColumn_family(column_family);
                    for (KeySlice key : keys) {

                        List<ColumnOrSuperColumn> line = key.getColumns();


                    }
                    if (keys.size() < search_slice_ratio) {
                        terminated = true;
                    }
                }
            } catch (TimedOutException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InvalidRequestException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (UnavailableException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            }

        }

        public Map<String, Integer> readCart(String cart) {

            TreeMap<String, List<Column>> columns = getAllColumnsFromSuperCF("ShoppingCart", cart + "", TRANSACTIONAL_CONSISTENCY_LEVEL);
            Map<String, Integer> result = new TreeMap<String, Integer>();

            for (String cartline : columns.keySet()) {

                String book_id = cartline;
                int qty = 0;

                for (Column c : columns.get(cartline)) {
                    if (new String(c.name).equals("QTY")) {
                        qty = (Integer) BenchmarkUtil.toObject(c.getValue());
                    }
                }

                result.put(book_id, qty);
            }

            return result;

        }

        public void addToCart(String cart, String item, int qty_to_add) {


            Object o = readfromSuperColumn(cart + "", "ShoppingCart", "QTY", item, TRANSACTIONAL_CONSISTENCY_LEVEL);

            if (o != null) {
                int qty;
                qty = (Integer) o;
                qty_to_add += qty;
            }
            insertInSuperColumn(qty_to_add, cart + "", "ShoppingCart", item, "QTY", TRANSACTIONAL_CONSISTENCY_LEVEL);
        }

        public BuyingResult BuyCartItem(String item, int qty) {

            Object o = readfromColumn(item, "Items", "I_STOCK", TRANSACTIONAL_CONSISTENCY_LEVEL);   //read stock
            if (o == null) {
                return BuyingResult.DOES_NOT_EXIST;
            }
            long stock = (Long) o;
            if ((stock - qty) >= 0) {   //if stock is sufficient
                stock -= qty;
                insert(stock, item, "Items", "I_STOCK", TRANSACTIONAL_CONSISTENCY_LEVEL);  //buy
                Object new_o = readfromColumn(item, "Items", "I_STOCK", TRANSACTIONAL_CONSISTENCY_LEVEL);  //read to see if available
                if (new_o == null) {
                    return BuyingResult.DOES_NOT_EXIST;
                }
                long result = (Long) new_o;
                if (result < 0) {
                    System.out.println("VICTORY");
                    return BuyingResult.OUT_OF_STOCK;
                }
            } else {
                return BuyingResult.NOT_AVAILABLE;
            }
            return BuyingResult.BOUGHT;
        }


        public Map<String, Map<String, Map<String, Object>>> getResults() {

            String column_family = "Results";
            Map<String, Map<String, Map<String, Object>>> results = new TreeMap<String, Map<String, Map<String, Object>>>();
            try {
                SlicePredicate predicate = new SlicePredicate();
                SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, 1000);

                ColumnParent parent = new ColumnParent();
                parent.setColumn_family(column_family);
                predicate.setSlice_range(range);

                String last_key = "";

                boolean terminated = false;

                while (!terminated) {
                    List<KeySlice> keys = getCassandraClient().get_range_slice(Keyspace, parent, predicate, last_key, "", search_slice_ratio, ConsistencyLevel.ONE);
                    delay();

                    if (keys.isEmpty()) {
                        System.out.println("The key range is empty");
                    } else {
                        last_key = keys.get(keys.size() - 1).key;
                    }
                    ColumnPath path = new ColumnPath();
                    path.setColumn_family(column_family);
                    for (KeySlice key : keys) {
                        if (!key.columns.isEmpty()) {
                            String key_name = key.key;    //for each client
                            if(!results.containsKey(key_name)){
                                results.put(key_name,new TreeMap<String, Map<String, Object>>());
                            }
                            for (ColumnOrSuperColumn c : key.getColumns()) {
                                if (c.isSetSuper_column()) {
                                    String superColumn_name = null;    //for each item
                                    try {
                                        superColumn_name = new String(c.getSuper_column().getName(),"UTF-8");
                                    } catch (UnsupportedEncodingException e) {
                                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                                    }
                                    if(!results.get(key_name).containsKey(superColumn_name)){
                                        results.get(key_name).put(superColumn_name,new TreeMap<String,Object>());
                                    }
                                    for (Column co : c.getSuper_column().columns) {
                                             results.get(key_name).get(superColumn_name).put(new String(co.getName()),BenchmarkUtil.toObject(co.getValue()));
                                    }
                                }
                            }
                        }

                    }
                    if (keys.size() < search_slice_ratio) {
                        terminated = true;
                    }
                }
                   return results;

            } catch (TimedOutException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InvalidRequestException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (UnavailableException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            }
                    return results;

        }
    }


}
