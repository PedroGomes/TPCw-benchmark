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

package org.uminho.gsd.benchmarks.TPCW_Cassandra.database;


import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.uminho.gsd.benchmarks.TPCW_Cassandra.entities.*;
import org.uminho.gsd.benchmarks.TPCW_Cassandra.populator.SchemaUtils;
import org.uminho.gsd.benchmarks.benchmark.BenchmarkNodeID;
import org.uminho.gsd.benchmarks.dataStatistics.ResultHandler;
import org.uminho.gsd.benchmarks.generic.BuyingResult;
import org.uminho.gsd.benchmarks.helpers.BenchmarkUtil;
import org.uminho.gsd.benchmarks.helpers.TPM_counter;
import org.uminho.gsd.benchmarks.helpers.ThinkTime;
import org.uminho.gsd.benchmarks.interfaces.Entity;
import org.uminho.gsd.benchmarks.interfaces.KeyGenerator;
import org.uminho.gsd.benchmarks.interfaces.Workload.Operation;
import org.uminho.gsd.benchmarks.interfaces.Workload.WorkloadGeneratorInterface;
import org.uminho.gsd.benchmarks.interfaces.executor.DatabaseExecutorInterface;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;


/**
 * TPCw execution class for cassandra
 */
public class TPCWCassandraExecutor implements DatabaseExecutorInterface {

	/**
	 * The global executer counter
	 */
	private static int global_executor_counter = 0;


	/**
	 * The executor id
	 */
	private int executor_id = 0;

	/**
	 * Cassandra nodes connection info*
	 */
	private Map<String, Integer> connections;
	/**
	 * Cassandra node clients*
	 */
	private ArrayList<Cassandra.Client> clients;
	/**
	 * Last used client - load balancing purposes *
	 */
	int last = 0;
	/**
	 * Keyspace name*
	 */
	private String keyspace;

	/**
	 * Think time*
	 */
	private long simulatedDelay;
	/**
	 * The number of keys to fetch from the database in each iteration*
	 */
	private int search_slice_ratio;

	/**
	 * This client result logger*
	 */
	ResultHandler client_result_handler;

	/**
	 * Insert consistency level*
	 */
	public static ConsistencyLevel INSERT_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
	/**
	 * Remove consistency level*
	 */
	public static ConsistencyLevel REMOVE_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
	/**
	 * Range consistency level*
	 */
	public static ConsistencyLevel RANGE_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
	/**
	 * Consistency level used in transactional like operations*
	 */
	public static ConsistencyLevel TRANSACTIONAL_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
	/**
	 * Read consistency level*
	 */
	public static ConsistencyLevel READ_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
	/**
	 * Write consistency level*
	 */
	public static ConsistencyLevel WRITE_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

	/**
	 * The items bought within the client*
	 */
	Map<String, Integer> partialBought = new TreeMap<String, Integer>();
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
	 * Generator of ids
	 */
	KeyGenerator keyGenerator;


	private TPM_counter counter;


	/**
	 * String decoders
	 */
	private static Charset charset = Charset.forName("UTF-8");


	/**
	 * Know Cfamilies
	 */
	List<String> keyspace_column_families;


	ArrayList<String> operations = new ArrayList<String>();

	/**
	 * TPCW VARIABLES*
	 */
	private String[] credit_cards = {"VISA", "MASTERCARD", "DISCOVER", "AMEX", "DINERS"};
	private String[] ship_types = {"AIR", "UPS", "FEDEX", "SHIP", "COURIER", "MAIL"};
	private String[] status_types = {"PROCESSING", "SHIPPED", "PENDING", "DENIED"};

	private Random random = new Random();

	/**
	 * Mapping for super columns*
	 */
	private Map<String, String> paths;


	/**
	 * A new TPCW-Casssandra client
	 *
	 * @param keyspace
	 * @param connections
	 * @param consistencyLevels
	 * @param think_time
	 * @param search_slices
	 */
	public TPCWCassandraExecutor(String keyspace, Map<String, Integer> connections, ConsistencyLevel[] consistencyLevels, Map<String, String> key_paths, int think_time, int search_slices, KeyGenerator keyGenerator, TPM_counter tpm_counter) {

		this.keyspace = keyspace;
		this.keyGenerator = keyGenerator;
		this.connections = connections;
		INSERT_CONSISTENCY_LEVEL = consistencyLevels[0];
		REMOVE_CONSISTENCY_LEVEL = consistencyLevels[1];
		RANGE_CONSISTENCY_LEVEL = consistencyLevels[2];
		TRANSACTIONAL_CONSISTENCY_LEVEL = consistencyLevels[3];
		READ_CONSISTENCY_LEVEL = consistencyLevels[4];
		WRITE_CONSISTENCY_LEVEL = consistencyLevels[5];
		this.clients = new ArrayList<Cassandra.Client>();
		this.connections = connections;
		this.keyspace = keyspace;
		this.paths = key_paths;
		this.counter = tpm_counter;

		Map<String, Integer> sortedConnections = BenchmarkUtil.randomizeMap(connections);

		for (String rand_host : sortedConnections.keySet()) {
			String host = rand_host.split(":")[1];

			try {

				TTransport transport;
				TProtocol protocol;
				Cassandra.Client client;


				transport = new TFramedTransport(new TSocket(host, connections.get(host)));
				transport.open();
				protocol = new org.apache.thrift.protocol.TBinaryProtocol(transport);
				client = new Cassandra.Client(protocol);
				clients.add(client);
				client.set_keyspace(keyspace);

			} catch (TTransportException ex) {
				System.out.println("[ERROR] FAILED TO CONNECT TO:" + host + ":port:" + connections.get(host) + " -> CLIENT IGNORED ");
			} catch (TException e) {
				e.printStackTrace();
			} catch (InvalidRequestException e) {
				e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
			}


		}

		if (clients.isEmpty()) {
			System.out.println("No available connections");
			System.exit(0);
		}

		keyspace_column_families = new ArrayList<String>();
		try {

			KsDef keyspace_def = getCassandraClient().describe_keyspace(keyspace);
			List<CfDef> CF_def = keyspace_def.getCf_defs();

			for (CfDef cfDef : CF_def) {
				keyspace_column_families.add(cfDef.name);

			}

		} catch (NotFoundException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		} catch (UnavailableException e) {
			e.printStackTrace();
		} catch (InvalidRequestException e) {
			e.printStackTrace();
		}

		search_slice_ratio = search_slices;
		simulatedDelay = think_time;

	}

	public void setSchema(List<Map<String, String>> column_families) throws Exception {
		SchemaUtils.createSchema(keyspace, column_families, clients);


	}


	public Cassandra.Client getCassandraClient() throws UnavailableException {

		boolean openClient = false;
		Cassandra.Client cl = null;

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

		if (cl == null) {
			throw new UnavailableException();

		}

		return cl;

	}

	public void closeClient() {
		for (Cassandra.Client client : clients) {
			try {
				client.getOutputProtocol().getTransport().flush();
				client.getOutputProtocol().getTransport().close();
			} catch (TTransportException e) {
				e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
			}
		}
		clients = null;
		connections = null;

		System.gc();
	}

	public Map<String, String> getInfo() {
		TreeMap<String, String> info = new TreeMap<String, String>();
		info.put("connections", connections.toString());
		return info;
	}

	public void start(WorkloadGeneratorInterface workload, BenchmarkNodeID nodeId, int operation_number, ResultHandler handler) {

		global_executor_counter++;
		executor_id = global_executor_counter;

		client_result_handler = handler;


		for (int operation = 0; operation < operation_number; operation++) {
			long g_init_time = System.currentTimeMillis();


			try {
				long init_time = System.currentTimeMillis();

				Operation op = workload.getNextOperation();
				executeMethod(op);
				long end_time = System.currentTimeMillis();
				client_result_handler.logResult(op.getOperation(), (end_time - init_time));

				simulatedDelay = ThinkTime.getThinkTime();

				if (simulatedDelay > 0) {
					Thread.sleep(simulatedDelay);
				}


			} catch (NoSuchFieldException e) {
				System.out.println("[ERROR:] THIS OPERATION DOES NOT EXIST: " + e.getMessage());
				break;
			} catch (InterruptedException e) {
				System.out.println("[ERROR:] THINK TIME AFTER METHOD EXECUTION INTERRUPTED: " + e.getMessage());
				break;

			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("-- Error : Client " + executor_id + " going down....");
				break;

			}
			long end_time = System.currentTimeMillis();
			counter.increment();
			client_result_handler.logResult("OPERATIONS", (end_time - g_init_time));


		}


		client_result_handler.getResulSet().put("bought", partialBought);
		client_result_handler.getResulSet().put("total_bought", bought_qty);
		client_result_handler.getResulSet().put("buying_actions", bought_actions);
		client_result_handler.getResulSet().put("bought_carts", bought_carts);
		client_result_handler.getResulSet().put("zeros", zeros);

	}

	public void execute(Operation op) throws Exception {
		executeMethod(op);
	}

	public void executeMethod(Operation op) throws Exception {

		if (op == null) {
			System.out.println("[ERROR]: NULL OPERATION");
			return;
		}

		operations.add(op.getOperation());

		String method_name = op.getOperation();


		if (method_name.equalsIgnoreCase("GET_STOCK_AND_PRODUCTS")) {
			ArrayList<String> fields = new ArrayList<String>();
			fields.add("I_TITLE");
			fields.add("I_STOCK");
			Map<String, Map<String, Object>> items_info = rangeQuery("Item", fields, -1);


			op.setResult(items_info);

		} else if (method_name.equalsIgnoreCase("Get_Stock_And_Products_after_increment")) {

			ArrayList<String> fields = new ArrayList<String>();
			int stock = (Integer) op.getParameter("STOCK");
			setItemStocks(stock);

			System.out.println("sleeeping after stock reposition...");
			Thread.sleep(60000);

			fields.add("I_STOCK");
			Map<String, Map<String, Object>> items_info = rangeQuery("item", fields, -1);
			op.setResult(items_info);


		} else if (method_name.equalsIgnoreCase("GET_ITEM_STOCK")) {

			String item_id = (String) op.getParameters().get("ITEM_ID");

			//long init_time = System.currentTimeMillis();
			Object o = read(item_id, "item", "I_STOCK", null);
			int stock = -1;
			if (o != null) {
				stock = (Integer) o;
			}
			// long end_time = System.currentTimeMillis();
			// client_result_handler.logResult("READ ITEM INFO", (end_time - init_time));
			op.setResult(stock);

		} else if (method_name.equalsIgnoreCase("ADD_TO_CART")) {

			String cart = (String) op.getParameter("CART_ID");
			String item_id = (String) op.getParameter("ITEM_ID");
			int qty = (Integer) op.getParameter("QTY");

			//  long init_time = System.currentTimeMillis();
			addToCart(cart, item_id, qty);
			//  long end_time = System.currentTimeMillis();
			//   client_result_handler.logResult("ADD ITEM TO CART", (end_time - init_time));


		} else if (method_name.equalsIgnoreCase("BUY_CART")) {

			bought_carts++;
			String cart_id = (String) op.getParameter("CART_ID");

			Customer c = null;

			if (op.getParameters().containsKey("Customer")) {
				c = (Customer) op.getParameter("Customer");
			}
			buyCart(cart_id, c);


//        } else if (method_name.equalsIgnoreCase("TOP_SELLERS")) {
//            getTopSellers(BEST_SELLERS_NUM);
//        } else if (method_name.equalsIgnoreCase("SEARCH_ITEM_BY_SUBJECT")) {
//
//            String subject = (String) op.getParameter("SUBJECT");
//            readItemWhere("item", "I_SUBJECT", subject);
//
//        } else if (method_name.equalsIgnoreCase("SEARCH_ITEM_BY_TITLE")) {
//            String title = (String) op.getParameter("TITLE");
//            readItemWhere("item", "I_TITLE", title);
//
//        } else if (method_name.equalsIgnoreCase("SEARCH_ITEM_BY_AUTHOR")) {
//
//            String name = (String) op.getParameter("AUTHOR");
//            String author_id = readAuthorIDWhere("author", "A_LNAME", name);
//            readItemWhere("item", "I_A_ID", author_id);
//
//        } else if (method_name.equalsIgnoreCase("NEW_PRODUCTS")) {
//
//            String subject = (String) op.getParameter("SUBJECT");
//            getNewProducts(subject);

		} else if (method_name.equalsIgnoreCase("GET_BENCHMARK_RESULTS")) {
			op.setResult(getResults());

		} else if (method_name.equalsIgnoreCase("OP_HOME")) {

			int costumer = (Integer) op.getParameter("COSTUMER");
			int item_id = (Integer) op.getParameter("ITEM");
			HomeOperation(costumer, item_id);
		} else if (method_name.equalsIgnoreCase("OP_SHOPPING_CART")) {

			String cart = (String) op.getParameter("CART");
			int item_id = (Integer) op.getParameter("ITEM");
			boolean create = (Boolean) op.getParameter("CREATE");
			shoppingCartInteraction(item_id, create, cart);


		} else if (method_name.equalsIgnoreCase("OP_REGISTER")) {
			String customer = (String) op.getParameter("CUSTOMER");


			CustomerRegistration(customer);

		} else if (method_name.equalsIgnoreCase("OP_LOGIN")) {

			String customer = (String) op.getParameter("CUSTOMER");

			refreshSession(customer);


		} else if (method_name.equalsIgnoreCase("OP_BUY_REQUEST")) {

			String id = (String) op.getParameter("CART");

			BuyRequest(id);

		} else if (method_name.equalsIgnoreCase("OP_BUY_CONFIRM")) {

			String car_id = (String) op.getParameter("CART");
			String costumer = (String) op.getParameter("CUSTOMER");


			BuyComfirm(costumer, car_id);


		} else if (method_name.equalsIgnoreCase("OP_ORDER_INQUIRY")) {
			String costumer = (String) op.getParameter("CUSTOMER");

			OrderInquiry(costumer);

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

	public BuyingResult buyItem(String item_id, int qty) throws Exception {
		long init_time = System.currentTimeMillis();
		BuyingResult result = BuyCartItem(item_id, qty);
		long end_time = System.currentTimeMillis();
		client_result_handler.logResult("BUY_ITEM", (end_time - init_time));
		List<Object> buying_information = new LinkedList<Object>();
		buying_information.add(qty);
		buying_information.add(init_time);
		buying_information.add(end_time);
		client_result_handler.record_unstructured_data("BOUGHT_ITEMS_TIMELINE", item_id + "", buying_information);

		return result;
	}

	public Map<String, Integer> fetchCart(String cart_id) throws Exception {
		long init_time = System.currentTimeMillis();
		Map<String, Integer> cart = readCart(cart_id);
		long end_time = System.currentTimeMillis();
		client_result_handler.logResult("FETCH CART " + cart.size(), (end_time - init_time));
		return cart;
	}


	/**
	 * *****************
	 * DataBaseCRUDInterface OPERATIONS *
	 * *****************
	 */

	public void insertOrder(Order order, List<OrderLine> orderLines, CCXact ccXact) throws Exception {

		List<ColumnOrSuperColumn> order_order_lines = new ArrayList<ColumnOrSuperColumn>();

		ColumnOrSuperColumn order_info = getSuperColumn_to_insert("order_info", order);
		order_order_lines.add(order_info);

		for (OrderLine orderLine : orderLines) {


			String ol_key = orderLine.getOL_ID();

			ColumnOrSuperColumn order_line_info = getSuperColumn_to_insert(ol_key, orderLine);
			order_order_lines.add(order_line_info);

		}
		batch_mutate(order.getO_ID(), "orders", order_order_lines, TRANSACTIONAL_CONSISTENCY_LEVEL);


		ColumnOrSuperColumn col = getSuperColumn_to_insert(order.getO_ID(), ccXact);
		List<ColumnOrSuperColumn> mutation = new ArrayList<ColumnOrSuperColumn>();
		mutation.add(col);
		batch_mutate(order.getO_C_ID(), "cc_xacts", mutation, WRITE_CONSISTENCY_LEVEL);


	}

	public ColumnOrSuperColumn getSuperColumn_to_insert(String super_c, Entity value) throws Exception {

		TreeMap<String, Object> fieldsToInsert;
		fieldsToInsert = value.getValuesToInsert();


		ArrayList<ColumnOrSuperColumn> columns = new ArrayList<ColumnOrSuperColumn>();


		List<Column> SC_columns = new ArrayList<Column>();
		for (String valueName : fieldsToInsert.keySet()) {
			Column column_to_insert = null;

			column_to_insert = new Column();
			column_to_insert.setName(valueName.getBytes("UTF-8"));
			column_to_insert.setValue(BenchmarkUtil.getBytes(fieldsToInsert.get(valueName)));
			column_to_insert.setTimestamp(System.currentTimeMillis());

			SC_columns.add(column_to_insert);
			// insertInSuperColumn(fieldsToInsert.get(valueName), ColumnFamilyKey, column, key, valueName, INSERT_CONSISTENCY_LEVEL);
		}
		SuperColumn value_superColumn = null;
		try {


			value_superColumn = new SuperColumn();
			value_superColumn.setName(super_c.getBytes("UTF-8"));
			value_superColumn.setColumns(SC_columns);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}
		ColumnOrSuperColumn column_or_superColumn = new ColumnOrSuperColumn();
		column_or_superColumn.setSuper_column(value_superColumn);


		return column_or_superColumn;
	}

	public Object insert(String key, String path, Entity value) throws Exception {

		TreeMap<String, Object> fieldsToInsert;
		fieldsToInsert = value.getValuesToInsert();

		String ColumnFamilyKey = "";

		ArrayList<ColumnOrSuperColumn> columns = new ArrayList<ColumnOrSuperColumn>();

		if (paths.containsKey(path)) {

			ColumnFamilyKey = (String) fieldsToInsert.get(paths.get(path));

			List<Column> SC_columns = new ArrayList<Column>();
			for (String valueName : fieldsToInsert.keySet()) {
				Column column_to_insert = null;

				//column_to_insert = new Column(valueName.getBytes(), BenchmarkUtil.getBytes(fieldsToInsert.get(valueName)), System.currentTimeMillis());
				column_to_insert = new Column();
				column_to_insert.setName(valueName.getBytes("UTF-8"));
				column_to_insert.setValue(BenchmarkUtil.getBytes(fieldsToInsert.get(valueName)));
				column_to_insert.setTimestamp(System.currentTimeMillis());


				SC_columns.add(column_to_insert);
				// insertInSuperColumn(fieldsToInsert.get(valueName), ColumnFamilyKey, column, key, valueName, INSERT_CONSISTENCY_LEVEL);
			}
			SuperColumn value_superColumn = null;
			try {


				//value_superColumn = new SuperColumn(key.getBytes("UTF-8"), SC_columns);
				value_superColumn = new SuperColumn();
				value_superColumn.setName(key.getBytes("UTF-8"));
				value_superColumn.setColumns(SC_columns);

			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
			}
			ColumnOrSuperColumn column_or_superColumn = new ColumnOrSuperColumn();
			column_or_superColumn.setSuper_column(value_superColumn);
			columns.add(column_or_superColumn);

		} else {

			ColumnFamilyKey = key;
			for (String valueName : fieldsToInsert.keySet()) {
				//Column column = new Column(valueName.getBytes(), BenchmarkUtil.getBytes("UTF-8"fieldsToInsert.get(valueName)), System.currentTimeMillis());

				Column column = new Column();
				column.setName(valueName.getBytes("UTF-8"));
				column.setValue(BenchmarkUtil.getBytes(fieldsToInsert.get(valueName)));
				column.setTimestamp(System.currentTimeMillis());


				ColumnOrSuperColumn column_or_superColumn = new ColumnOrSuperColumn();
				column_or_superColumn.setColumn(column);
				columns.add(column_or_superColumn);
				// insert(fieldsToInsert.get(valueName), key, path, valueName, INSERT_CONSISTENCY_LEVEL);
			}
		}

		batch_mutate(ColumnFamilyKey, path, columns, INSERT_CONSISTENCY_LEVEL);		//      delay();


		return null;
	}

	public void update(String key, String path, String column, Object value, String superColumn) throws Exception {


		if (superColumn != null) {
			String ColumnFamily = path;

			insertInSuperColumn(value, key, path, superColumn, column, WRITE_CONSISTENCY_LEVEL);

		} else {
			insert(value, key, path, column, WRITE_CONSISTENCY_LEVEL);
		}

	}

	public Object read(String key, String path, String column, String superColumn) throws Exception {

		Object result = null;
		if (superColumn != null) {
			String columnFamily = path;
			result = readfromSuperColumn(key, columnFamily, column, superColumn, READ_CONSISTENCY_LEVEL);

		} else {
			result = readfromColumn(key, path, column, READ_CONSISTENCY_LEVEL);

		}
		return result;
	}

	public Map<String, Map<String, Object>> multiget(String path, List<String> keys, List<String> fields) throws Exception {

		ColumnParent parent = new ColumnParent(path);

		SlicePredicate predicate = new SlicePredicate();
		if (fields == null) {

			//, "".getBytes(), false, 2000
			SliceRange range = new SliceRange();
			range.setStart("".getBytes());
			range.setFinish("".getBytes("UTF-8"));
			range.setReversed(false);
			predicate.setSlice_range(range);
		} else {

			List<ByteBuffer> field_names = new ArrayList<ByteBuffer>();
			for (String field : fields) {

				field_names.add(ByteBuffer.wrap(field.getBytes("UTF-8")));
			}
			predicate.setColumn_names(field_names);
		}
		Map<ByteBuffer, List<ColumnOrSuperColumn>> query_result = null;

		List<java.nio.ByteBuffer> keys_bytes = new ArrayList<ByteBuffer>(keys.size());
		for (String key : keys) {
			keys_bytes.add(ByteBuffer.wrap(key.getBytes("UTF-8")));
		}


		query_result = getCassandraClient().multiget_slice(keys_bytes, parent, predicate, RANGE_CONSISTENCY_LEVEL);


		Map<String, Map<String, Object>> results = new TreeMap<String, Map<String, Object>>();

		if (query_result != null) {
			for (ByteBuffer key_byte : query_result.keySet()) {

				List<ColumnOrSuperColumn> retrieved_columns = query_result.get(key_byte);
				String key = charset.decode(key_byte).toString();

				for (ColumnOrSuperColumn c : retrieved_columns) {
					if (c.isSetSuper_column()) {


						String super_key = new String(c.super_column.getName(), "UTF-8");
						Map<String, Object> supercolumns = new TreeMap<String, Object>();
						for (Column co : c.getSuper_column().columns) {
							supercolumns.put(new String(co.getName(), "UTF-8"), BenchmarkUtil.toObject(co.getValue()));
						}

						if (!results.containsKey(key)) {
							Map<String, Object> returned_fields = new TreeMap<String, Object>();
							returned_fields.put(super_key, supercolumns);
							results.put(key, returned_fields);

						} else {
							results.get(key).put(super_key, supercolumns);
						}
					} else {

						if (!results.containsKey(key)) {
							Map<String, Object> returned_fields = new TreeMap<String, Object>();
							returned_fields.put(new String(c.getColumn().getName(), "UTF-8"), BenchmarkUtil.toObject(c.getColumn().getValue()));
							results.put(key, returned_fields);
						} else {
							results.get(key).put(new String(c.getColumn().getName(), "UTF-8"), BenchmarkUtil.toObject(c.getColumn().getValue()));
						}
					}
				}


			}
		}

		return results;
	}




	public Map<String, Map<String, Object>> rangeQuery(String column_family, List<String> fields, int limit) throws Exception {


		Map<String, Map<String, Object>> results = new TreeMap<String, Map<String, Object>>();
		long timeout = 0;


		try {
			SlicePredicate predicate = new SlicePredicate();

			if (fields == null) {

				//SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, 2000);

				SliceRange range = new SliceRange();
				range.setStart("".getBytes("UTF-8"));
				range.setFinish("".getBytes("UTF-8"));
				range.setReversed(false);

				predicate.setSlice_range(range);
			} else {


				List<ByteBuffer> field_names = new ArrayList<ByteBuffer>();
				for (String field : fields) {

					field_names.add(ByteBuffer.wrap(field.getBytes("UTF-8")));
				}
				predicate.setColumn_names(field_names);


			}


			KeyRange range = new KeyRange();
			range.setStart_key("".getBytes("UTF-8"));
			range.setEnd_key("".getBytes("UTF-8"));
			range.setCount(search_slice_ratio);

			ColumnParent parent = new ColumnParent();
			parent.setColumn_family(column_family);


			byte[] last_key = "".getBytes("UTF-8");

			boolean terminated = false;
			limit = (limit < 0) ? -1 : limit;
			int number_keys = 0;


			while (!terminated) {

				timeout = System.currentTimeMillis();
				List<KeySlice> keys = getCassandraClient().get_range_slices(parent, predicate, range, RANGE_CONSISTENCY_LEVEL);


				if (keys.isEmpty()) {
					System.out.println("The key range is empty");
				} else {
					last_key = keys.get(keys.size() - 1).getKey();
					range.setStart_key(last_key);
				}
				// Map<String, List<ColumnOrSuperColumn>> results = getCassandraClient().multiget_slice(Keyspace, keys, parent, predicate, ConsistencyLevel.ONE);

				for (KeySlice key : keys) {
					if (!key.columns.isEmpty()) {
						for (ColumnOrSuperColumn c : key.getColumns()) {
							if (c.isSetSuper_column()) {
								for (Column co : c.getSuper_column().columns) {
									String r_key = charset.decode(key.key).toString();

									if (!results.containsKey(r_key)) {
										Map<String, Object> returned_fields = new TreeMap<String, Object>();
										returned_fields.put(new String(co.getName(), "UTF-8"), BenchmarkUtil.toObject(co.getValue()));
										results.put(new String(key.getKey(), "UTF-8"), returned_fields);
									} else {
										results.get(r_key).put(new String(co.getName(), "UTF-8"), BenchmarkUtil.toObject(co.getValue()));
									}
								}
							} else {

								String r_key =  charset.decode(key.key).toString();
								if (!results.containsKey(r_key)) {
									Map<String, Object> returned_fields = new TreeMap<String, Object>();
									returned_fields.put(new String(c.getColumn().getName(), "UTF-8"), BenchmarkUtil.toObject(c.getColumn().getValue()));
									results.put(r_key, returned_fields);
								} else {
									results.get(r_key).put(new String(c.getColumn().getName(), "UTF-8"), BenchmarkUtil.toObject(c.getColumn().getValue()));
								}
							}
						}
						number_keys++;
					}
					if (number_keys >= limit && limit != -1) {
						terminated = true;
						break;
					}
				}
				if (keys.size() < search_slice_ratio) {
					terminated = true;
				}
			}


		} catch (TimedOutException ex) {
			System.out.println(">>Erro timeout: " + (System.currentTimeMillis() - timeout));
			throw ex;
		}

		return results;  //To change body of implemented methods use File | Settings | File Templates.
	}

	public Map<String, Map<String, Object>> modified_rangeQuery(String column_family, String key_name, List<String> fields, int limit) throws Exception {


		Map<String, Map<String, Object>> results = new TreeMap<String, Map<String, Object>>();
		long timeout = 0;

		List<KeySlice> retreived_keys = new LinkedList<KeySlice>();

		try {


			SlicePredicate key_predicate = new SlicePredicate();


			List<ByteBuffer> key_field_name = new ArrayList<ByteBuffer>();
			key_field_name.add(ByteBuffer.wrap(key_name.getBytes("UTF-8")));
			key_predicate.setColumn_names(key_field_name);


			KeyRange range = new KeyRange();
			range.setStart_key("".getBytes("UTF-8"));
			range.setEnd_key("".getBytes("UTF-8"));
			range.setCount(search_slice_ratio);

			ColumnParent parent = new ColumnParent();
			parent.setColumn_family(column_family);


			byte[] last_key = "".getBytes("UTF-8");

			boolean terminated = false;
			limit = (limit < 0) ? -1 : limit;
			int number_keys = 0;


			while (!terminated) {

				timeout = System.currentTimeMillis();
				List<KeySlice> keys = getCassandraClient().get_range_slices(parent, key_predicate, range, RANGE_CONSISTENCY_LEVEL);


				if (keys.isEmpty()) {
					System.out.println("The key range is empty");
				} else {
					last_key = keys.get(keys.size() - 1).getKey();
					range.setStart_key(last_key);
				}

				for (KeySlice key : keys) {
					if (!key.getColumns().isEmpty()) {
						number_keys++;
						retreived_keys.add(key);
					}
					if (number_keys >= limit && limit != -1) {
						terminated = true;
						break;
					}

				}

				if (keys.size() < search_slice_ratio) {
					terminated = true;
				}
			}

		} catch (TimedOutException ex) {
			System.out.println(">>Erro timeout: " + (System.currentTimeMillis() - timeout));
			throw ex;
		}

		for (KeySlice key : retreived_keys) {
			if (!key.columns.isEmpty()) {


				String object_key = new String(key.getKey(), "UTF-8");

				//	String object_key = key.getKey();

				Map<String, Object> retrieved_columns = this.getColumnMap(object_key, column_family, fields, READ_CONSISTENCY_LEVEL);
				results.put(object_key, retrieved_columns);
			}
		}

		return results;
	}

	public Map<String, Map<String, Map<String, Object>>> super_rangeQuery(String column_family, List<String> fields, int limit) throws Exception {

		Map<String, Map<String, Map<String, Object>>> results = new TreeMap<String, Map<String, Map<String, Object>>>();
		long timeout = 0;


		try {
			SlicePredicate predicate = new SlicePredicate();

			if (fields == null) {

				SliceRange range = new SliceRange();
				range.setStart("".getBytes("UTF-8"));
				range.setFinish("".getBytes("UTF-8"));
				range.setReversed(false);

				predicate.setSlice_range(range);
			} else {


				List<ByteBuffer> field_names = new ArrayList<ByteBuffer>();
				for (String field : fields) {

					field_names.add(ByteBuffer.wrap(field.getBytes("UTF-8")));
				}
				predicate.setColumn_names(field_names);


			}


			KeyRange range = new KeyRange();
			range.setStart_key("".getBytes("UTF-8"));
			range.setEnd_key("".getBytes("UTF-8"));
			range.setCount(search_slice_ratio);

			ColumnParent parent = new ColumnParent();
			parent.setColumn_family(column_family);


			byte[] last_key = "".getBytes("UTF-8");

			boolean terminated = false;
			limit = (limit < 0) ? -1 : limit;
			int number_keys = 0;

			while (!terminated) {
				timeout = System.currentTimeMillis();
				List<KeySlice> keys = getCassandraClient().get_range_slices(parent, predicate, range, RANGE_CONSISTENCY_LEVEL);


				if (keys.isEmpty()) {
					System.out.println("The key range is empty");
				} else {
					last_key = keys.get(keys.size() - 1).getKey();
					range.setStart_key(last_key);
				}
				// Map<String, List<ColumnOrSuperColumn>> results = getCassandraClient().multiget_slice(Keyspace, keys, parent, predicate, ConsistencyLevel.ONE);

				for (KeySlice key : keys) {
					if (!key.columns.isEmpty()) {
						Map<String, Map<String, Object>> returned_fields = new TreeMap<String, Map<String, Object>>();
						results.put(new String(key.getKey(), "UTF-8"), returned_fields);
						for (ColumnOrSuperColumn c : key.getColumns()) {
							if (c.isSetSuper_column()) {
								if (!c.getSuper_column().columns.isEmpty()) {

									Map<String, Object> columns = new TreeMap<String, Object>();

									for (Column co : c.getSuper_column().columns) {
										String column_name = new String(co.getName(), "UTF-8");
										Object value = BenchmarkUtil.toObject(co.getValue());
										columns.put(column_name, value);
									}
									String super_key = new String(c.getSuper_column().getName(), "UTF-8");
									returned_fields.put(super_key, columns);
								}
							}
						}
						number_keys++;
					}
					if (number_keys >= limit && limit != -1) {
						terminated = true;
						break;
					}
				}
				if (keys.size() < search_slice_ratio) {
					terminated = true;
				}
			}


		} catch (TimedOutException ex) {
			System.out.println(">>Erro timeout: " + (System.currentTimeMillis() - timeout));
			throw ex;
		}

		return results;  //To change body of implemented methods use File | Settings | File Templates.
	}

	public Map<String, Map<String, Map<String, Object>>> modified_super_rangeQuery(String column_family, String key_name, List<String> know_field, int limit) throws Exception {


		Map<String, Map<String, Map<String, Object>>> results = new TreeMap<String, Map<String, Map<String, Object>>>();
		long timeout = 0;

		List<KeySlice> retreived_keys = new LinkedList<KeySlice>();

		try {


			SlicePredicate key_predicate = new SlicePredicate();


			List<ByteBuffer> key_field_name = new ArrayList<ByteBuffer>();
			key_field_name.add(ByteBuffer.wrap(key_name.getBytes("UTF-8")));
			key_predicate.setColumn_names(key_field_name);


			KeyRange range = new KeyRange();
			range.setStart_key("".getBytes("UTF-8"));
			range.setEnd_key("".getBytes("UTF-8"));
			range.setCount(search_slice_ratio);

			ColumnParent parent = new ColumnParent();
			parent.setColumn_family(column_family);


			byte[] last_key = "".getBytes("UTF-8");

			boolean terminated = false;
			limit = (limit < 0) ? -1 : limit;
			int number_keys = 0;


			while (!terminated) {

				timeout = System.currentTimeMillis();
				List<KeySlice> keys = getCassandraClient().get_range_slices(parent, key_predicate, range, RANGE_CONSISTENCY_LEVEL);


				if (keys.isEmpty()) {
					System.out.println("The key range is empty");
				} else {
					last_key = keys.get(keys.size() - 1).getKey();
					range.setStart_key(last_key);
				}

				for (KeySlice key : keys) {
					if (!key.getColumns().isEmpty()) {
						number_keys++;
						retreived_keys.add(key);
					}
					if (number_keys >= limit && limit != -1) {
						terminated = true;
						break;
					}

				}

				if (keys.size() < search_slice_ratio) {
					terminated = true;
				}
			}

		} catch (TimedOutException ex) {
			System.out.println(">>Erro timeout: " + (System.currentTimeMillis() - timeout));
			throw ex;
		}


		for (KeySlice key : retreived_keys) {

			if (!key.columns.isEmpty()) {


				String object_key = new String(key.getKey(), "UTF-8");

				Map<String, Map<String, Object>> retreived_super_columns = getAllColumnsMapFromSuperCF(column_family, object_key, READ_CONSISTENCY_LEVEL);
				results.put(object_key, retreived_super_columns);
			}
		}

		return results;

	}

	public void remove(String key, String columnFamily, String column) throws Exception {
		//ColumnPath path = new ColumnPath(column, null, null);
		ColumnPath path = new ColumnPath();
		path.setColumn_family(columnFamily);
		path.setColumn(column.getBytes("UTF-8"));

		getCassandraClient().remove(ByteBuffer.wrap(key.getBytes("UTF-8")), path, System.currentTimeMillis(), REMOVE_CONSISTENCY_LEVEL);
		//delay();

	}

	public void remove_super(String key, String columnFamily, String super_column) throws Exception {
		//ColumnPath path = new ColumnPath(column, null, null);
		ColumnPath path = new ColumnPath();
		path.setColumn_family(columnFamily);
		path.setSuper_column(super_column.getBytes("UTF-8"));
		getCassandraClient().remove(ByteBuffer.wrap(key.getBytes("UTF-8")), path, System.currentTimeMillis(), REMOVE_CONSISTENCY_LEVEL);
		//delay();
	}

	public void truncate(String column_family) throws Exception {

		column_family = column_family.toLowerCase();

		System.out.println("Removing ColumnFamily: " + column_family);

		if (!keyspace_column_families.contains(column_family)) {
			System.out.println("Cannot remove : " + column_family + " <- Not found!");
			return;
		}

		SlicePredicate predicate = new SlicePredicate();
		//	SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, 3000);
		SliceRange range = new SliceRange();
		range.setStart("".getBytes("UTF-8"));
		range.setFinish("".getBytes("UTF-8"));
		range.setReversed(false);

		ColumnParent parent = new ColumnParent();
		parent.setColumn_family(column_family);
		predicate.setSlice_range(range);

		byte[] last_key = "".getBytes("UTF-8");
		int alive_keys = 0;
		int total_keys = 0;
		boolean terminated = false;

		KeyRange krange = new KeyRange();
		krange.setStart_key(last_key);
		krange.setEnd_key("".getBytes("UTF-8"));
		krange.setCount(search_slice_ratio);

		while (!terminated) {
			Cassandra.Client c = getCassandraClient();
			List<KeySlice> keys = c.get_range_slices(parent, predicate, krange, RANGE_CONSISTENCY_LEVEL);
			//delay();

			if (keys.isEmpty()) {
				System.out.println("The key range is empty");
			} else {
				last_key = keys.get(keys.size() - 1).getKey();
				krange.setStart_key(last_key);
			}
			// Map<String, List<ColumnOrSuperColumn>> results = getCassandraClient().multiget_slice(Keyspace, keys, parent, predicate, ConsistencyLevel.ONE);
			ColumnPath path = new ColumnPath();
			path.setColumn_family(column_family);
			for (KeySlice key : keys) {


				if (!key.columns.isEmpty()) {
					getCassandraClient().remove(key.key, path, System.currentTimeMillis(), REMOVE_CONSISTENCY_LEVEL);
					alive_keys++;
				}
				total_keys++;
			}
			if (keys.size() < search_slice_ratio) {
				terminated = true;
			}
		}
		System.out.println("[Column family " + column_family + "] total keys:" + total_keys + " alive keys:" + alive_keys);
	}

	public void index(String key, String path, Object value) throws Exception {
		insert(new byte[]{1}, key, path, value.toString(), WRITE_CONSISTENCY_LEVEL);
	}

	public void index(String key, String path, String indexed_key, Map<String, Object> value) throws Exception {

		List<Column> index_columns = new ArrayList<Column>();

		for (Map.Entry<String, Object> entry : value.entrySet()) {
			String name = entry.getKey();
			Object data = entry.getValue();

			Column col = new Column();
			col.setName(name.getBytes("UTF-8"));
			col.setValue(BenchmarkUtil.getBytes(data));
			col.setTimestamp(System.currentTimeMillis());


			//	Column col = new Column(name.getBytes(), BenchmarkUtil.getBytes(data), System.currentTimeMillis());
			index_columns.add(col);
		}
		SuperColumn superColumn = new SuperColumn(ByteBuffer.wrap(indexed_key.getBytes("UTF-8")), index_columns);

		ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
		columnOrSuperColumn.setSuper_column(superColumn);
		List<ColumnOrSuperColumn> mutations = new ArrayList<ColumnOrSuperColumn>();
		mutations.add(columnOrSuperColumn);
		batch_mutate(key, path, mutations, WRITE_CONSISTENCY_LEVEL);

	}

	/**
	 * ******************
	 * DATA BASE METHODS *
	 * *******************
	 */

	public void batch_mutate(String key, String columnFamily, List<ColumnOrSuperColumn> mutation_columns, ConsistencyLevel level) throws Exception {

		List<Mutation> mutations_List = new ArrayList<Mutation>();

		for (ColumnOrSuperColumn cols : mutation_columns) {
			Mutation mutation = new Mutation();
			mutation.setColumn_or_supercolumn(cols);
			mutations_List.add(mutation);
		}

		Map<String, List<Mutation>> mutations_cf = new TreeMap<String, List<Mutation>>();
		mutations_cf.put(columnFamily, mutations_List);
		Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new TreeMap<ByteBuffer, Map<String, List<Mutation>>>();
		mutationMap.put(ByteBuffer.wrap(key.getBytes("UTF-8")), mutations_cf);

		TreeMap<String, List<ColumnOrSuperColumn>> mutations = new TreeMap<String, List<ColumnOrSuperColumn>>();
		mutations.put(columnFamily, mutation_columns);
		getCassandraClient().batch_mutate(mutationMap, level);
	}

	public void batch_mutate_columns(String key, String columnFamily, List<Column> mutation_columns, ConsistencyLevel level) throws Exception {

		List<Mutation> mutations_List = new ArrayList<Mutation>();

		for (Column cols : mutation_columns) {

			Mutation mutation = new Mutation();
			ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
			columnOrSuperColumn.setColumn(cols);
			mutation.setColumn_or_supercolumn(columnOrSuperColumn);
			mutations_List.add(mutation);
		}

		Map<String, List<Mutation>> mutations_cf = new TreeMap<String, List<Mutation>>();
		mutations_cf.put(columnFamily, mutations_List);
		Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new TreeMap<ByteBuffer, Map<String, List<Mutation>>>();
		mutationMap.put(ByteBuffer.wrap(key.getBytes("UTF-8")), mutations_cf);

		//TreeMap<String, List<ColumnOrSuperColumn>> mutations = new TreeMap<String, List<ColumnOrSuperColumn>>();
		//mutations.put(columnFamily, mutation_columns);
		getCassandraClient().batch_mutate(mutationMap, level);
	}

	public void insert(Object value, String key, String column_family, String column_name, ConsistencyLevel writeConsistency) throws Exception {


		ColumnParent parent = new ColumnParent();
		parent.setColumn_family(column_family);

		Column column = new Column();
		column.setName(column_name.getBytes("UTF-8"));
		column.setValue(BenchmarkUtil.getBytes(value));
		column.setTimestamp(System.currentTimeMillis());

		getCassandraClient().insert(ByteBuffer.wrap(key.getBytes("UTF-8")), parent, column, writeConsistency);
	}

	public void insertInSuperColumn(Object value, String key, String column_family, String SuperColumn, String Column, ConsistencyLevel writeConsistency) throws Exception {
		// ColumnPath path = new ColumnPath(column_family, SuperColumn.getBytes(), Column.getBytes());
		ColumnParent parent = new ColumnParent();
		parent.setColumn_family(column_family);
		parent.setSuper_column(SuperColumn.getBytes("UTF-8"));

		Column column = new Column();
		column.setName(Column.getBytes("UTF-8"));
		column.setValue(BenchmarkUtil.getBytes(value));
		column.setTimestamp(System.currentTimeMillis());


		getCassandraClient().insert(ByteBuffer.wrap(key.getBytes("UTF-8")), parent, column, writeConsistency);
	}

	public Object readfromColumn(String key, String ColumnFamily, String column, ConsistencyLevel con) throws Exception {
		ColumnPath path = new ColumnPath();
		path.setColumn_family(ColumnFamily);
		path.setColumn(column.getBytes("UTF-8"));
		byte[] value = null;
		try {
			value = getCassandraClient().get(ByteBuffer.wrap(key.getBytes("UTF-8")), path, con).column.getValue();
		} catch (NotFoundException e) {
			return null;
		}

		Object o = BenchmarkUtil.toObject(value);
		//delay();
		return o;

	}

	public Object readfromSuperColumn(String familykey, String columnFamily, String column, String superColumnKey, ConsistencyLevel con) throws Exception {

		//   ColumnPath path = new ColumnPath(columnFamily, superColumnKey.getBytes(), column.getBytes());
		ColumnPath path = new ColumnPath(columnFamily);
		path.setSuper_column(superColumnKey.getBytes("UTF-8"));
		path.setColumn(column.getBytes("UTF-8"));

		byte[] value = null;
		try {
			value = getCassandraClient().get(ByteBuffer.wrap(familykey.getBytes("UTF-8")), path, con).column.getValue();
		} catch (NotFoundException e) {
			return null;
		}

		Object o;
		o = BenchmarkUtil.toObject(value);
		//delay();
		return o;


	}

	private List<ColumnOrSuperColumn> getListColumns(String key, String columnFamily, String superColumn, List<String> column_names, ConsistencyLevel con) throws Exception {

		ColumnParent parent = new ColumnParent(columnFamily);
		if (superColumn != null) {
			parent.setSuper_column(superColumn.getBytes("UTF-8"));
		}


		SlicePredicate fields = new SlicePredicate();


		List<ByteBuffer> field_names = new ArrayList<ByteBuffer>();
		for (String field : column_names) {

			field_names.add(ByteBuffer.wrap(field.getBytes("UTF-8")));
		}
		fields.setColumn_names(field_names);


		List<ColumnOrSuperColumn> columns = getCassandraClient().get_slice(ByteBuffer.wrap(key.getBytes("UTF-8")), parent, fields, con);
		return columns;


	}

	private Map<String, Object> getColumnMap(String key, String columnFamily, List<String> column_names, ConsistencyLevel con) throws Exception {

		ColumnParent parent = new ColumnParent(columnFamily);


		SlicePredicate predicate = new SlicePredicate();

		if (column_names == null) {

			//SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, 2000);
			SliceRange range = new SliceRange();
			range.setStart("".getBytes("UTF-8"));
			range.setFinish("".getBytes("UTF-8"));
			range.setReversed(false);

			predicate.setSlice_range(range);
		} else {


			List<ByteBuffer> field_names = new ArrayList<ByteBuffer>();
			for (String field : column_names) {

				field_names.add(ByteBuffer.wrap(field.getBytes("UTF-8")));
			}
			predicate.setColumn_names(field_names);

		}


		List<ColumnOrSuperColumn> columns = getCassandraClient().get_slice(ByteBuffer.wrap(key.getBytes("UTF-8")), parent, predicate, con);

		Map<String, Object> value_map = new TreeMap<String, Object>();

		for (ColumnOrSuperColumn columnOrSuperColumn : columns) {
			Column column = columnOrSuperColumn.getColumn();
			String name = new String(column.getName(), "UTF-8");
			Object value = BenchmarkUtil.toObject(column.getValue());
			value_map.put(name, value);


		}
		return value_map;

	}

	private TreeMap<String, Map<String, Object>> getMappedColumnsFromSuperCF(String path, String key, List<String> names, int limit, ConsistencyLevel consistencyLevel) throws Exception {

		ColumnParent parent = new ColumnParent();
		parent.setColumn_family(path);

		SlicePredicate predicate = new SlicePredicate();


//        List<byte[]> column_names = new ArrayList<byte[]>();
//        for (String name : names) {
//            column_names.add(name.getBytes());
//        }
		// predicate.setColumn_names(column_names);

		SliceRange range = new SliceRange();
		range.setStart("".getBytes("UTF-8"));
		range.setFinish("".getBytes("UTF-8"));
		range.setReversed(false);
		range.setCount(limit);


		predicate.setSlice_range(range);

		List<ColumnOrSuperColumn> superColumns = getCassandraClient().get_slice(ByteBuffer.wrap(key.getBytes("UTF-8")), parent, predicate, consistencyLevel);

		TreeMap<String, Map<String, Object>> result = new TreeMap<String, Map<String, Object>>();
		for (ColumnOrSuperColumn sc : superColumns) {

			List<Column> columns = sc.getSuper_column().getColumns();

			Map<String, Object> value_map = new TreeMap<String, Object>();

			for (Column column : columns) {
				String name = new String(column.getName(), "UTF-8");
				if (names.contains(name)) {
					Object value = BenchmarkUtil.toObject(column.getValue());
					value_map.put(name, value);


				}


			}


			result.put(new String(sc.super_column.getName(), "UTF-8"), value_map);

		}
		//delay();
		return result;
	}

	private TreeMap<String, List<Column>> getAllColumnsFromSuperCF(String path, String key, ConsistencyLevel consistencyLevel) throws Exception {

		ColumnParent parent = new ColumnParent();
		parent.setColumn_family(path);

		SlicePredicate predicate = new SlicePredicate();

		//SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, 300);
		SliceRange range = new SliceRange();
		range.setStart("".getBytes("UTF-8"));
		range.setFinish("".getBytes("UTF-8"));
		range.setReversed(false);

		predicate.setSlice_range(range);


		List<ColumnOrSuperColumn> superColumns = getCassandraClient().get_slice(ByteBuffer.wrap(key.getBytes("UTF-8")), parent, predicate, consistencyLevel);


		TreeMap<String, List<Column>> result = new TreeMap<String, List<Column>>();
		for (ColumnOrSuperColumn sc : superColumns) {

			List<Column> columns = sc.getSuper_column().getColumns();

			result.put(new String(sc.super_column.getName(), "UTF-8"), columns);

		}
		//delay();
		return result;

	}

	private TreeMap<String, Map<String, Object>> getAllColumnsMapFromSuperCF(String path, String key, ConsistencyLevel consistencyLevel) throws Exception {

		ColumnParent parent = new ColumnParent();
		parent.setColumn_family(path);

		SlicePredicate predicate = new SlicePredicate();

//		SliceRange range = new SliceRange("".getBytes("UTF-8"), "".getBytes("UTF-8"), false, 500);
		SliceRange range = new SliceRange();
		range.setStart("".getBytes("UTF-8"));
		range.setFinish("".getBytes("UTF-8"));
		range.setReversed(false);
		range.setCount(500);
		predicate.setSlice_range(range);

		TreeMap<String, Map<String, Object>> value_map = new TreeMap<String, Map<String, Object>>();
		boolean terminated = false;


		while (!terminated) {

			List<ColumnOrSuperColumn> result = getCassandraClient().get_slice(ByteBuffer.wrap(key.getBytes("UTF-8")), parent, predicate, consistencyLevel);
			if (result.size() < 500) {
				terminated = true;
			} else {
				byte[] last_column = result.get(result.size() - 1).getSuper_column().getName();
				range.setStart(last_column);
				predicate.setSlice_range(range);
			}


			for (ColumnOrSuperColumn columnOrSuperColumn : result) {
				SuperColumn superColumn = columnOrSuperColumn.getSuper_column();
				Map<String, Object> map = new TreeMap<String, Object>();
				for (Column col : superColumn.getColumns()) {
					String name = new String(col.getName(), "UTF-8");
					Object value = BenchmarkUtil.toObject(col.getValue());
					map.put(name, value);
				}
				String super_name = new String(superColumn.getName(), "UTF-8");
				value_map.put(super_name, map);
			}

		}


		return value_map;

	}

	private TreeMap<String, Map<String, Object>> getAllColumnsMapFromSuperCFLimit(String path, String key, int limit, ConsistencyLevel consistencyLevel) throws Exception {

		ColumnParent parent = new ColumnParent();
		parent.setColumn_family(path);

		SlicePredicate predicate = new SlicePredicate();

//		SliceRange range = new SliceRange("".getBytes(), "".getBytes(), false, limit);
		SliceRange range = new SliceRange();
		range.setStart("".getBytes("UTF-8"));
		range.setFinish("".getBytes("UTF-8"));
		range.setReversed(false);
		range.setCount(limit);

		predicate.setSlice_range(range);

		TreeMap<String, Map<String, Object>> value_map = new TreeMap<String, Map<String, Object>>();


		List<ColumnOrSuperColumn> result = getCassandraClient().get_slice(ByteBuffer.wrap(key.getBytes("UTF-8")), parent, predicate, consistencyLevel);

		for (ColumnOrSuperColumn columnOrSuperColumn : result) {
			SuperColumn superColumn = columnOrSuperColumn.getSuper_column();
			Map<String, Object> map = new TreeMap<String, Object>();
			for (Column col : superColumn.getColumns()) {
				String name = new String(col.getName(), "UTF-8");
				Object value = BenchmarkUtil.toObject(col.getValue());
				map.put(name, value);
			}
			String super_name = new String(superColumn.getName(), "UTF-8");
			value_map.put(super_name, map);
		}

		return value_map;

	}


	/********************************************************/
	/****  TPCW benchmark consistency and old operations ****/
	/**
	 * ****************************************************
	 */

	public void setItemStocks(int initial_stock) throws Exception {


		ArrayList<String> fields = new ArrayList<String>();
		fields.add("I_STOCK");
		Map<String, Map<String, Object>> items_info = rangeQuery("item", fields, -1);


		for (String key : items_info.keySet()) {
			insert(initial_stock, key, "item", "I_STOCK", ConsistencyLevel.ALL);
		}


	}

	public Map<String, Integer> readCart(String cart) throws Exception {

		TreeMap<String, List<Column>> columns = getAllColumnsFromSuperCF("shopping_cart_line", cart + "", TRANSACTIONAL_CONSISTENCY_LEVEL);
		Map<String, Integer> result = new TreeMap<String, Integer>();

		for (String cartline : columns.keySet()) {

			String book_id = cartline;
			int qty = 0;

			for (Column c : columns.get(cartline)) {
				if (new String(c.getName(), "UTF-8").equals("QTY")) {
					qty = (Integer) BenchmarkUtil.toObject(c.getValue());
				}
			}

			result.put(book_id, qty);
		}

		return result;

	}

	public void addToCart(String cart, String item, int qty_to_add) throws Exception {


		Object o = readfromSuperColumn(cart + "", "shopping_cart_line", "QTY", item, TRANSACTIONAL_CONSISTENCY_LEVEL);

		if (o != null) {
			int qty;
			qty = (Integer) o;
			qty_to_add += qty;
		}
		insertInSuperColumn(qty_to_add, cart + "", "shopping_cart_line", item, "QTY", TRANSACTIONAL_CONSISTENCY_LEVEL);
	}

	int debug_bougth_items = 0;
	int total_bought = 0;

	public BuyingResult BuyCartItem(String item, int qty) throws Exception {
		total_bought++;

		try {

			Object o = readfromColumn(item, "item", "I_STOCK", TRANSACTIONAL_CONSISTENCY_LEVEL);   //read stock
			if (o == null) {
				return BuyingResult.DOES_NOT_EXIST;
			}
			int stock = (Integer) o;
			if ((stock - qty) >= 0) {   //if stock is sufficient

				if (stock - qty == 0) {
					zeros++;
				}

				stock -= qty;
				insert(stock, item, "item", "I_STOCK", TRANSACTIONAL_CONSISTENCY_LEVEL);  //buy

//            Object new_o = readfromColumn(item, "Items", "I_STOCK", TRANSACTIONAL_CONSISTENCY_LEVEL);  //read to see if available
//            if (new_o == null) {
//                return TPCWBenchmarkInterface.BuyingResult.DOES_NOT_EXIST;
//            }
//            long result = (Long) new_o;
//            if (result < 0) {
//                System.out.println("VICTORY... NOT");
//                return TPCWsim BenchmarkInterface.BuyingResult.OUT_OF_STOCK;
//            }
			} else {
				return BuyingResult.NOT_AVAILABLE;
			}
			debug_bougth_items++;

//			if (total_bought == 225) {
//				System.out.println("BI: " + debug_bougth_items);
//			}
			return BuyingResult.BOUGHT;

		} catch (Exception e) {
			return BuyingResult.CANT_COMFIRM;
		}

	}

	public Map<String, Map<String, Map<String, Object>>> getResults() throws Exception {

		String column_family = "results";
		Map<String, Map<String, Map<String, Object>>> results = new TreeMap<String, Map<String, Map<String, Object>>>();
		SlicePredicate predicate = new SlicePredicate();

		SliceRange slice_range = new SliceRange();
		slice_range.setStart("".getBytes("UTF-8"));
		slice_range.setFinish("".getBytes("UTF-8"));
		slice_range.setReversed(false);

		//SliceRange slice_range = new SliceRange("".getBytes(), "".getBytes(), false, 10500);


		ColumnParent parent = new ColumnParent();
		parent.setColumn_family(column_family);
		predicate.setSlice_range(slice_range);

		byte[] last_key = "".getBytes("UTF-8");


		KeyRange range = new KeyRange();
		range.setStart_key("".getBytes("UTF-8"));
		range.setEnd_key(last_key);
		range.setCount(search_slice_ratio);

		boolean terminated = false;

		while (!terminated) {
			List<KeySlice> keys = getCassandraClient().get_range_slices(parent, predicate, range, ConsistencyLevel.ONE);
			//delay();

			if (keys.isEmpty()) {
				System.out.println("[INFO|CASSANDRA:]The key range is empty");
			} else {
				last_key = keys.get(keys.size() - 1).getKey();
				range.setStart_key(last_key);
			}
			ColumnPath path = new ColumnPath();
			path.setColumn_family(column_family);
			for (KeySlice key : keys) {
				if (!key.columns.isEmpty()) {
					String key_name = charset.decode(key.key).toString();	//for each client
					if (!results.containsKey(key_name)) {
						results.put(key_name, new TreeMap<String, Map<String, Object>>());
					}
					for (ColumnOrSuperColumn c : key.getColumns()) {
						if (c.isSetSuper_column()) {
							String superColumn_name = null;	//for each item
							try {
								superColumn_name = new String(c.getSuper_column().getName(), "UTF-8");
							} catch (UnsupportedEncodingException e) {
								e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
							}
							if (!results.get(key_name).containsKey(superColumn_name)) {
								results.get(key_name).put(superColumn_name, new TreeMap<String, Object>());
							}
							for (Column co : c.getSuper_column().columns) {
								results.get(key_name).get(superColumn_name).put(new String(co.getName(), "UTF-8"), BenchmarkUtil.toObject(co.getValue()));
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


	}

	public void buyCart(String cart_id, Customer c) throws Exception {


		Map<String, Integer> cart = fetchCart(cart_id);
//		Map<String, Integer> bought_items = new TreeMap<String, Integer>();
		for (String item : cart.keySet()) {

			BuyingResult result = buyItem(item, cart.get(item));
			client_result_handler.countEvent("BUYING_COUNTERS", result.name(), 1);


			if (result.equals(BuyingResult.BOUGHT)) {
//				bought_items.put(item, cart.get(item));
				bought_qty += cart.get(item);
				bought_actions++;
				if (!partialBought.containsKey(item)) {
					partialBought.put(item, cart.get(item));

				} else {
					int bought = partialBought.get(item);
					partialBought.put(item, (cart.get(item) + bought));
				}

			}
		}

//		if (!bought_items.isEmpty() && c != null) {
//			//      insertOrder(bought_items, c);
//		}
	}


	/**************************************/
	/****  TPCW benchmark operations  ****/
	/**
	 * ********************************
	 */


	//Receives an Costumer id and retrieves is  name
	//Receives a item and returns the id and thumbnail of related.
	public void HomeOperation(int costumer, int item) throws Exception {


		List<String> costumer_fields_names = new ArrayList<String>();
		costumer_fields_names.add("C_FNAME");
		costumer_fields_names.add("C_LNAME");

		List<ColumnOrSuperColumn> cl = getListColumns(costumer + "", "customer", null, costumer_fields_names, READ_CONSISTENCY_LEVEL);

		List<String> item_fields_names = new ArrayList<String>();
		item_fields_names.add("I_RELATED1");
		item_fields_names.add("I_RELATED2");
		item_fields_names.add("I_RELATED3");
		item_fields_names.add("I_RELATED4");
		item_fields_names.add("I_RELATED5");

		List<ColumnOrSuperColumn> items = getListColumns(item + "", "item", null, item_fields_names, READ_CONSISTENCY_LEVEL);


		for (ColumnOrSuperColumn column : items) {
			int item_related = (Integer) BenchmarkUtil.toObject(column.getColumn().getValue());


			Object x = readfromColumn(item_related + "", "item", "I_THUMBNAIL", READ_CONSISTENCY_LEVEL);
			//      System.out.println("X:" + x);
		}


	}


	public void shoppingCartInteraction(int item, boolean create, String cart_id) throws Exception {

		//if create cart
		if (create) {
			Timestamp stamp = new Timestamp(System.currentTimeMillis());
			insertInSuperColumn(stamp, cart_id, "shopping_cart", "cart_info", "SC_DATE", WRITE_CONSISTENCY_LEVEL);
		}

		//add item

		//if exists


		List<String> columns = new ArrayList<String>();
		columns.add("I_COST");
		columns.add("I_SRP");
		columns.add("I_TITLE");
		columns.add("I_BACKING");


		float i_cost = 0;
		double i_srp = 0;
		String i_title = "";
		String i_backing = "";


		List<ColumnOrSuperColumn> item_data = getListColumns(item + "", "item", null, columns, READ_CONSISTENCY_LEVEL);
		for (ColumnOrSuperColumn column : item_data) {
			Column item_col = column.getColumn();
			String name = new String(item_col.getName(), "UTF-8");
			Object obj = BenchmarkUtil.toObject(item_col.getValue());

			if (name.equals("I_COST")) {

				if (obj instanceof Double) {
					double costd = (Double) obj;
					i_cost = (float) costd;
				}
				if (obj instanceof Float) {
					i_cost = (Float) obj;
				}


			}
			if (name.equals("I_SRP")) {
				i_srp = (Double) obj;

			}
			if (name.equals("I_TITLE")) {
				i_title = (String) obj;

			}
			if (name.equals("I_BACKING")) {
				i_backing = (String) obj;

			}
		}


		Object o = readfromSuperColumn(cart_id, "shopping_cart", "SCL_QTY", item + "", TRANSACTIONAL_CONSISTENCY_LEVEL);
		int qty = 1;
		if (o != null) {

			qty = (Integer) o;
			qty++;
		}


		List<Column> column_insert = new ArrayList<Column>();


		long timestamp = System.currentTimeMillis();

		Column cost = new Column();
		cost.setName("SCL_COST".getBytes("UTF-8"));
		cost.setValue(BenchmarkUtil.getBytes(i_cost));
		cost.setTimestamp(timestamp);


		Column srp = new Column();
		srp.setName("SCL_SRP".getBytes("UTF-8"));
		srp.setValue(BenchmarkUtil.getBytes(i_srp));
		srp.setTimestamp(timestamp);


		Column title = new Column();
		title.setName("SCL_TITLE".getBytes("UTF-8"));
		title.setValue(BenchmarkUtil.getBytes(i_title));
		title.setTimestamp(timestamp);

		Column back = new Column();
		back.setName("SCL_BACKING".getBytes("UTF-8"));
		back.setValue(BenchmarkUtil.getBytes(i_backing));
		back.setTimestamp(timestamp);

		Column qty_col = new Column();
		qty_col.setName("SCL_QTY".getBytes("UTF-8"));
		qty_col.setValue(BenchmarkUtil.getBytes(qty));
		qty_col.setTimestamp(timestamp);

		column_insert.add(cost);
		column_insert.add(srp);
		column_insert.add(title);
		column_insert.add(back);
		column_insert.add(qty_col);


		SuperColumn s_column = new SuperColumn();
		s_column.setName((item + "").getBytes("UTF-8"));
		s_column.setColumns(column_insert);

		ColumnOrSuperColumn superColumn = new ColumnOrSuperColumn();
		superColumn.setSuper_column(s_column);
		List<ColumnOrSuperColumn> mutations = new ArrayList<ColumnOrSuperColumn>();
		mutations.add(superColumn);
		batch_mutate(cart_id, "shopping_cart", mutations, TRANSACTIONAL_CONSISTENCY_LEVEL);

		//insertInSuperColumn(qty, cart_id, "shopping_cart", item + "", "QTY", TRANSACTIONAL_CONSISTENCY_LEVEL);
	}


	public void CustomerRegistration(String costumer_id) throws Exception {

		String name = (BenchmarkUtil.getRandomAString(8, 13) + " " + BenchmarkUtil.getRandomAString(8, 15));
		String[] names = name.split(" ");
		Random r = new Random();
		int random_int = r.nextInt(1000);

		String key = names[0] + "_" + (costumer_id);


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


		String address_id = insertAdress();
		//insert(address.getAddr_id(), key, "Customer", "C_ADDR_ID", writeCon);

		Customer c = new Customer(costumer_id, key, pass, last_name, first_name, phone + "", email, C_SINCE, C_LAST_LOGIN, C_LOGIN, C_EXPIRATION, C_BALANCE, C_YTD_PMT, C_BIRTHDATE, C_DATA, discount, address_id);

		insert(costumer_id, "customer", c);


	}


	public String insertAdress() throws Exception {


		String ADDR_STREET1, ADDR_STREET2, ADDR_CITY, ADDR_STATE;
		String ADDR_ZIP;
		int country_id;

		ADDR_STREET1 = "street" + BenchmarkUtil.getRandomAString(10, 30);


		ADDR_STREET2 = "street" + BenchmarkUtil.getRandomAString(10, 30);
		ADDR_CITY = BenchmarkUtil.getRandomAString(4, 30);
		ADDR_STATE = BenchmarkUtil.getRandomAString(2, 20);
		ADDR_ZIP = BenchmarkUtil.getRandomAString(5, 10);
		country_id = BenchmarkUtil.getRandomInt(0, 92 - 1);


		String key = ADDR_STREET1 + ADDR_STREET2 + ADDR_CITY + ADDR_STATE + ADDR_ZIP + country_id;

		org.uminho.gsd.benchmarks.TPCW_Cassandra.entities.Address address = new Address(key, ADDR_STREET1, ADDR_STREET2, ADDR_CITY,
				ADDR_STATE, ADDR_ZIP, country_id);
//            insert(ADDR_STREET1, key, "Addresses", "ADDR_STREET1", writeConsistency);
//            insert(ADDR_STREET2, key, "Addresses", "ADDR_STREET2", writeConsistency);
//            insert(ADDR_STATE, key, "Addresses", "ADDR_STATE", writeConsistency);
//            insert(ADDR_CITY, key, "Addresses", "ADDR_CITY", writeConsistency);
//            insert(ADDR_ZIP, key, "Addresses", "ADDR_ZIP", writeConsistency);
//            insert(country.getCo_id(), key, "Addresses", "ADDR_CO_ID", writeConsistency);


		Object o = readfromColumn(key, "address", "ADDR_ZIP", READ_CONSISTENCY_LEVEL);

		if (o == null) {   //does not exists
			insert(key, "address", address);
		}

		return key;
	}

	public void refreshSession(String C_ID) throws Exception {


		List<String> columns = new ArrayList<String>();
		columns.add("C_PASSWD");
		columns.add("C_FNAME ");
		columns.add("C_LNAME ");
		columns.add("C_PHONE ");
		columns.add("C_EMAIL ");
		columns.add("C_BIRTHDATE ");
		columns.add("C_DISCOUNT ");

		List<ColumnOrSuperColumn> cust_columns = getListColumns(C_ID, "customer", null, columns, READ_CONSISTENCY_LEVEL);
		for (ColumnOrSuperColumn column : cust_columns) {
			if ((new String(column.getColumn().getName(), "UTF-8")).equals("C_ADDR_ID")) {
				String add_id = (String) BenchmarkUtil.toObject(column.getColumn().getValue());
				columns = new ArrayList<String>();
				columns.add("ADDR_STREET1");
				columns.add("ADDR_STREET2 ");
				columns.add("ADDR_CITY ");
				columns.add("ADDR_STATE ");
				columns.add("ADDR_ZIP ");
				getListColumns(add_id, "address", null, columns, READ_CONSISTENCY_LEVEL);
			}


		}

		long timestamp = System.currentTimeMillis();

		Column login = new Column();
		login.setName("C_LOGIN".getBytes("UTF-8"));
		login.setValue(BenchmarkUtil.getBytes(new Timestamp(System.currentTimeMillis())));
		login.setTimestamp(timestamp);


		Column expirantion = new Column();
		expirantion.setName("C_EXPIRATION".getBytes("UTF-8"));
		expirantion.setValue(BenchmarkUtil.getBytes(new Timestamp(System.currentTimeMillis() + 7200000)));
		expirantion.setTimestamp(timestamp);

		ColumnOrSuperColumn login_col = new ColumnOrSuperColumn().setColumn(login);
		ColumnOrSuperColumn expiration_col = new ColumnOrSuperColumn().setColumn(login);

		ArrayList<ColumnOrSuperColumn> muColumns = new ArrayList<ColumnOrSuperColumn>();
		muColumns.add(login_col);
		muColumns.add(expiration_col);

		batch_mutate(C_ID, "customer", muColumns, TRANSACTIONAL_CONSISTENCY_LEVEL);


	}


	public void BuyRequest(String shopping_id) {

		operations.add("REQUEST ENTRY: " + shopping_id);

		try {

			List<String> names = new ArrayList<String>();
			names.add("SCL_QTY");
			names.add("SCL_COST");

			Map<String, Map<String, Object>> items = getMappedColumnsFromSuperCF("shopping_cart", shopping_id, names, 100, READ_CONSISTENCY_LEVEL);

			float total_cost = 0;
			int total_qty = 0;


			for (String superColumnName : items.keySet()) {
				if (superColumnName != "cart_info") {
					Map<String, Object> columns = items.get(superColumnName);
					float cost = 0f;
					int qty = 0;

					for (Map.Entry col : columns.entrySet()) {
						String name = (String) col.getKey();
						Object value = col.getValue();


						if (name.equals("SCL_COST")) {

							if (value instanceof Double) {
								double costd = (Double) value;
								cost = (float) costd;
							}
							if (value instanceof Float) {
								cost = (Float) value;
							}
						}

						if (name.equals("SCL_QTY")) {
							qty = (Integer) value;

						}

					}

					total_qty += qty;
					total_cost += (cost * qty);
				}
			}

			float SC_SUB_TOTAL = total_cost * (1 - 0.2f);//cheats...
			float SC_TAX = SC_SUB_TOTAL * 0.0825f;
			float SC_SHIP_COST = 3.00f + (1.00f * total_qty);
			float SC_TOTAL = SC_SUB_TOTAL + SC_SHIP_COST + SC_TAX;


			List<Column> column_insert = new ArrayList<Column>();


			long timestamp = System.currentTimeMillis();

			Column sub_total = new Column();
			sub_total.setName("SC_SUB_TOTAL".getBytes("UTF-8"));
			sub_total.setValue(BenchmarkUtil.getBytes(SC_SUB_TOTAL));
			sub_total.setTimestamp(timestamp);


			Column tax = new Column();
			tax.setName("SC_TAX".getBytes("UTF-8"));
			tax.setValue(BenchmarkUtil.getBytes(SC_TAX));
			tax.setTimestamp(timestamp);

			Column ship = new Column();
			ship.setName("SC_SHIP_COST".getBytes("UTF-8"));
			ship.setValue(BenchmarkUtil.getBytes(SC_SHIP_COST));
			ship.setTimestamp(timestamp);

			Column total = new Column();
			total.setName("SC_TOTAL".getBytes("UTF-8"));
			total.setValue(BenchmarkUtil.getBytes(SC_TOTAL));
			total.setTimestamp(timestamp);

			Column date = new Column();
			date.setName("SC_DATE".getBytes("UTF-8"));
			date.setValue(BenchmarkUtil.getBytes(new Timestamp(System.currentTimeMillis())));
			date.setTimestamp(timestamp);

			column_insert.add(sub_total);
			column_insert.add(tax);
			column_insert.add(ship);
			column_insert.add(total);
			column_insert.add(date);


			SuperColumn s_column = new SuperColumn();
			s_column.setName("cart_info".getBytes("UTF-8"));
			s_column.setColumns(column_insert);

			ColumnOrSuperColumn superColumn = new ColumnOrSuperColumn();
			superColumn.setSuper_column(s_column);
			List<ColumnOrSuperColumn> mutations = new ArrayList<ColumnOrSuperColumn>();
			mutations.add(superColumn);
			batch_mutate(shopping_id, "shopping_cart", mutations, TRANSACTIONAL_CONSISTENCY_LEVEL);
			operations.add("CONFIRMED: " + SC_TOTAL);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public void BuyComfirm(String customer, String cart) throws Exception {

		List<String> columns_to_retreive = new ArrayList<String>();
		columns_to_retreive.add("SCL_QTY");
		columns_to_retreive.add("SCL_COST");
		columns_to_retreive.add("SC_SUB_TOTAL");
		columns_to_retreive.add("SC_TAX");
		columns_to_retreive.add("SC_SHIP_COST");
		columns_to_retreive.add("SC_TOTAL");

		Map<String, Map<String, Object>> shoppingCart = getMappedColumnsFromSuperCF("shopping_cart", cart, columns_to_retreive, 100, READ_CONSISTENCY_LEVEL);

		columns_to_retreive.clear();

		columns_to_retreive.add("C_FNAME");
		columns_to_retreive.add("C_LNAME");
		columns_to_retreive.add("C_ADDR_ID");
		columns_to_retreive.add("C_DISCOUNT");

		Map<String, Object> customer_info = getColumnMap(customer, "customer", columns_to_retreive, READ_CONSISTENCY_LEVEL);
		double c_discount = (Double) customer_info.get("C_DISCOUNT");


		String ship_addr_id = "";
		String cust_addr = (String) customer_info.get("C_ADDR_ID");

		float decision = random.nextFloat();
		if (decision < 0.2) {
			ship_addr_id = insertAdress();
		} else {
			ship_addr_id = cust_addr;
		}


		String[] ids = cart.split("\\.");
		int thread_id = Integer.parseInt(ids[1]);

		String shipping = ship_types[random.nextInt(ship_types.length)];

		float total = 0;
		try {
			total = (Float) shoppingCart.get("cart_info").get("SC_TOTAL");
		} catch (Exception e) {
			System.out.println("Null pointer on cart:" + cart);
			System.out.println("OPERATIONS: " + operations.toString());
			e.printStackTrace();
			return;
		}

		String order_id = enterOrder(customer, thread_id, shoppingCart, ship_addr_id, cust_addr, shipping, total, c_discount);

		String cc_type = BenchmarkUtil.getRandomAString(10);
		long cc_number = BenchmarkUtil.getRandomNString(16);
		String cc_name = BenchmarkUtil.getRandomAString(30);
		Date cc_expiry = new Date(System.currentTimeMillis() + random.nextInt(644444400));

		enterCCXact(order_id, customer, cc_type, cc_number, cc_name, cc_expiry, total, ship_addr_id);


	}


	public String enterOrder(String customer_id, int thread_id, Map<String, Map<String, Object>> shoppingCart, String ship_addr_id, String cust_addr, String shipping, float total, double c_discount) throws Exception {


		String key = (String) keyGenerator.getNextKey(thread_id);

		float subTotal = (Float) shoppingCart.get("cart_info").get("SC_SUB_TOTAL");
		float tax = (Float) shoppingCart.get("cart_info").get("SC_TAX");
		float ship = (Float) shoppingCart.get("cart_info").get("SC_SHIP_COST");
		String shipType = ship_types[random.nextInt(ship_types.length)];
		String status = status_types[random.nextInt(status_types.length)];


		Order order = new Order(key, customer_id, new Date(System.currentTimeMillis()), subTotal, tax, total, shipType, new Date(System.currentTimeMillis() + random.nextInt(604800000)), status, cust_addr, ship_addr_id);
		List<ColumnOrSuperColumn> order_order_lines = new ArrayList<ColumnOrSuperColumn>();

		ColumnOrSuperColumn order_info = getSuperColumn_to_insert("order_info", order);
		order_order_lines.add(order_info);

		int index = 0;
		for (String item : shoppingCart.keySet()) {

			if (!item.equals("cart_info")) {

				int qty = (Integer) shoppingCart.get(item).get("SCL_QTY");
				int item_id = Integer.parseInt(item.trim());

				String ol_key = key + "." + index;

				OrderLine orderLine = new OrderLine(ol_key, key, item_id, qty, c_discount, BenchmarkUtil.getRandomAString(20, 100));//(id, OrderID, item, qty, discount, "");

				ColumnOrSuperColumn order_line_info = getSuperColumn_to_insert(ol_key, orderLine);
				order_order_lines.add(order_line_info);
				index++;

				int item_i = Integer.parseInt(item);
				Object o = null;
				o = readfromColumn(item, "item", "I_STOCK", TRANSACTIONAL_CONSISTENCY_LEVEL);

				if (o == null) {
					System.out.println("NULL ITEM:" + item);

				}
				int stock = (Integer) o;

				int new_stock = stock - item_i;
				insert(new_stock, item, "item", "I_STOCK", TRANSACTIONAL_CONSISTENCY_LEVEL);
			}
		}
		batch_mutate(key, "orders", order_order_lines, TRANSACTIONAL_CONSISTENCY_LEVEL);
		return key;
	}


	public void enterCCXact(String order_id, String customer, String cc_type, long cc_number,
							String cc_name, Date cc_expiry, float total, String ship_addr_id) throws Exception {

		int co_id = (Integer) readfromColumn(ship_addr_id, "address", "ADDR_CO_ID", READ_CONSISTENCY_LEVEL);
		CCXact ccxact = new CCXact(cc_type, cc_number, cc_name, cc_expiry, total, cc_expiry, order_id, co_id);

		ColumnOrSuperColumn col = getSuperColumn_to_insert(order_id, ccxact);
		List<ColumnOrSuperColumn> mutation = new ArrayList<ColumnOrSuperColumn>();
		mutation.add(col);
		batch_mutate(customer, "cc_xacts", mutation, WRITE_CONSISTENCY_LEVEL);
		///   insert(order_id, "cc_xacts", ccxact);
	}

	public void OrderInquiry(String customer) throws Exception {	   //1h

//        long t1 = System.currentTimeMillis();

		List<String> columns_to_retrieve_cust = new ArrayList<String>();
		columns_to_retrieve_cust.add("C_FNAME");
		columns_to_retrieve_cust.add("C_LNAME");
		columns_to_retrieve_cust.add("C_PHONE");
		columns_to_retrieve_cust.add("C_EMAIL");

		getListColumns(customer, "customer", null, columns_to_retrieve_cust, READ_CONSISTENCY_LEVEL);

		List<String> columns_to_retrieve_ccx = new ArrayList<String>();
		columns_to_retrieve_ccx.add("CX_TYPE");
		columns_to_retrieve_ccx.add("CX_CC_NUM");

		TreeMap<String, Map<String, Object>> cc_act = getMappedColumnsFromSuperCF("cc_xacts", customer, columns_to_retrieve_ccx, 1, READ_CONSISTENCY_LEVEL);
		if (!cc_act.isEmpty()) {
			String order_key = cc_act.firstKey();
			//   System.out.println("OR_K="+order_key);

			List<String> columns_to_retrieve_order = new ArrayList<String>();
			columns_to_retrieve_order.add("O_DATE");
			columns_to_retrieve_order.add("O_SUB_TOTAL");
			columns_to_retrieve_order.add("O_TAX");
			columns_to_retrieve_order.add("O_TOTAL");
			columns_to_retrieve_order.add("O_SHIP_TYPE");
			columns_to_retrieve_order.add("O_SHIP_DATE");
			columns_to_retrieve_order.add("O_STATUS");
			columns_to_retrieve_order.add("O_BILL_ADDR_ID");
			columns_to_retrieve_order.add("O_SHIP_ADDR_ID");
			columns_to_retrieve_order.add("OL_QTY");
			columns_to_retrieve_order.add("OL_DISCOUNT");
			columns_to_retrieve_order.add("OL_COMMENTS");

			Map<String, Map<String, Object>> orders = getMappedColumnsFromSuperCF("orders", order_key, columns_to_retrieve_order, 100, READ_CONSISTENCY_LEVEL);
			String addr_b_key = (String) orders.get("order_info").get("O_BILL_ADDR_ID");
			String addr_s_key = (String) orders.get("order_info").get("O_SHIP_ADDR_ID");

			List<String> items_keys = new ArrayList<String>();

			for (String item : orders.keySet()) {
				if (!item.equals("order_info")) {
					items_keys.add(item);
				}
			}

			List<String> columns_to_retrieve_addr = new ArrayList<String>();
			columns_to_retrieve_addr.add("ADDR_STREET1");
			columns_to_retrieve_addr.add("ADDR_STREET2");
			columns_to_retrieve_addr.add("ADDR_CITY");
			columns_to_retrieve_addr.add("ADDR_STATE");
			columns_to_retrieve_addr.add("ADDR_ZIP");
			columns_to_retrieve_addr.add("ADDR_CO_ID");

			List<String> keys = new ArrayList<String>();
			keys.add(addr_b_key);
			keys.add(addr_s_key);

			Map<String, Map<String, Object>> addrsMap = multiget("address", keys, columns_to_retrieve_addr);

			int co_b_id = (Integer) addrsMap.get(addr_b_key).get("ADDR_CO_ID");
			int co_s_id = (Integer) addrsMap.get(addr_s_key).get("ADDR_CO_ID");

			readfromColumn(co_b_id + "", "country", "CO_NAME", READ_CONSISTENCY_LEVEL);
			readfromColumn(co_s_id + "", "country", "CO_NAME", READ_CONSISTENCY_LEVEL);

			List<String> columns_to_retrieve_item = new ArrayList<String>();
			columns_to_retrieve_item.add("I_TITLE");
			columns_to_retrieve_item.add("I_PUBLISHER");
			columns_to_retrieve_item.add("I_COST");

			multiget("item", items_keys, columns_to_retrieve_item);
		}
	}


	public void doSearch(String term, String field) throws Exception {   //30


		if (term.equalsIgnoreCase("SUBJECT")) {
			Map<String, Map<String, Object>> columns = getAllColumnsMapFromSuperCF("item_subject_index", field, READ_CONSISTENCY_LEVEL);
			//          System.out.println("Subject search"+columns);


//            List<String> keys = new ArrayList<String>();
//            for (String key : columns.keySet()) {
//                keys.add(key);
//            }
//            List<String> columns_to_retrieve_item = new ArrayList<String>();
//            columns_to_retrieve_item.add("I_TITLE");
//            columns_to_retrieve_item.add("I_A_ID");
//            Map<String, Map<String, Object>> items = multiget("item", keys, columns_to_retrieve_item);
//
//            keys.clear();
//            for (Map<String, Object> values : items.values()) {
//                int a_id = (Integer) values.get("I_A_ID");
//                keys.add(a_id + "");
//            }
//
//
//            List<String> columns_to_retrieve_author = new ArrayList<String>();
//            columns_to_retrieve_author.add("ADDR_STREET1");
//            columns_to_retrieve_author.add("A_LNAME");
//
//            multiget("author", keys, columns_to_retrieve_item);

		} else if (term.equalsIgnoreCase("AUTHOR")) {
			Map<String, Map<String, Object>> columns = getAllColumnsMapFromSuperCF("item_author_index", field, READ_CONSISTENCY_LEVEL);
//            System.out.println("Author search"+columns);

//            List<String> keys = new ArrayList<String>();
//            for (String key : columns.keySet()) {
//                keys.add(key);
//            }
//            List<String> columns_to_retrieve_item = new ArrayList<String>();
//            columns_to_retrieve_item.add("I_TITLE");
//            columns_to_retrieve_item.add("I_A_ID");
//            Map<String, Map<String, Object>> items = multiget("item", keys, columns_to_retrieve_item);
//
//            keys.clear();
//            for (Map<String, Object> values : items.values()) {
//                int a_id = (Integer) values.get("I_A_ID");
//                keys.add(a_id + "");
//            }
//
//
//            List<String> columns_to_retrieve_author = new ArrayList<String>();
//            columns_to_retrieve_author.add("A_FNAME");

		} else if (term.equalsIgnoreCase("TITLE")) {
			Map<String, Map<String, Object>> columns = getAllColumnsMapFromSuperCF("item_title_index", field, READ_CONSISTENCY_LEVEL);
			//         System.out.println("Title search"+columns);
//            List<String> keys = new ArrayList<String>();
//            for (String key : columns.keySet()) {
//                keys.add(key);
//            }
//            List<String> columns_to_retrieve_item = new ArrayList<String>();
//            columns_to_retrieve_item.add("I_TITLE");
//            columns_to_retrieve_item.add("I_A_ID");
//            Map<String, Map<String, Object>> items = multiget("item", keys, columns_to_retrieve_item);
//
//            keys.clear();
//            for (Map<String, Object> values : items.values()) {
//                int a_id = (Integer) values.get("I_A_ID");
//                keys.add(a_id + "");
//            }
//
//            List<String> columns_to_retrieve_author = new ArrayList<String>();
//            columns_to_retrieve_author.add("A_FNAME");
//            columns_to_retrieve_author.add("A_LNAME");


		} else {
			System.out.println("OPTION NOT RECOGNIZED");
		}
	}

	public void newProducts(String field) throws Exception {   //1h
		Map<String, Map<String, Object>> columns = getAllColumnsMapFromSuperCFLimit("item_subject_index", field, 50, READ_CONSISTENCY_LEVEL);
	}

	public void BestSellers(String field) throws Exception {


		Map<String, Map<String, Map<String, Object>>> orders = super_rangeQuery("orders", null, 3333);
		// Map<String, Map<String, Map<String, Object>>> orders = modified_super_rangeQuery("orders", "order_info", null, 3333);
		//	System.out.println("retreived best sellers = " + orders.size());

		Map<Integer, Integer> items_info = new TreeMap<Integer, Integer>();

		HashSet<String> item_keys = new HashSet<String>();
		Map<Integer, Integer> valid_items = new TreeMap<Integer, Integer>();


		for (Map<String, Map<String, Object>> orders_info : orders.values()) {
			for (Map.Entry<String, Map<String, Object>> order_line : orders_info.entrySet()) {
				String super_column_name = order_line.getKey();
				if (!super_column_name.equals("order_info")) {

					Map<String, Object> columns = order_line.getValue();
					int item_id = (Integer) columns.get("OL_I_ID");
					int item_qty = (Integer) columns.get("OL_QTY");
					item_keys.add(item_id + "");

					if (items_info.containsKey(item_id)) {
						int current_qty = items_info.get(item_id);
						items_info.put(item_id, (item_qty + current_qty));
					} else {
						items_info.put(item_id, item_qty);
					}
				}
			}
		}

		List<String> columns_to_retrieve_item = new ArrayList<String>();
		columns_to_retrieve_item.add("I_TITLE");
		columns_to_retrieve_item.add("I_SUBJECT");
		columns_to_retrieve_item.add("I_A_ID");


		List<String> final_keys = new ArrayList<String>(item_keys.size());
		for (String item_key : item_keys) {
			final_keys.add(item_key);
		}

		Map<String, Map<String, Object>> items = multiget("item", final_keys, columns_to_retrieve_item);
		for (Map.Entry<String, Map<String, Object>> entry : items.entrySet()) {
			int id = Integer.parseInt(entry.getKey());
			String subject = (String) entry.getValue().get("I_SUBJECT");
			if (subject.equals(field)) {
				int author_id = (Integer) entry.getValue().get("I_A_ID");
				valid_items.put(id, author_id);
			}
		}

		Map<Integer, Integer> items_sells = new TreeMap<Integer, Integer>();
		ArrayList<String> author_keys = new ArrayList<String>();


		for (Integer id : valid_items.keySet()) {
			items_sells.put(id, items_info.get(id));
			author_keys.add(valid_items.get(id) + "");
		}

		Map top_sellers = reverseSortByValue(items_info);

		List<Integer> best = new ArrayList<Integer>();
		int num = 0;
		for (Iterator<Integer> it = top_sellers.keySet().iterator(); it.hasNext(); ) {
			int key = it.next();
			best.add(key);
			num++;
			if (num == 50)
				break;
		}

		List<String> columns_to_retrieve_author = new ArrayList<String>();
		columns_to_retrieve_author.add("A_FNAME");
		columns_to_retrieve_author.add("A_LNAME");

		multiget("author", author_keys, columns_to_retrieve_author);

	}

	public void ItemInfo(int id) throws Exception {

		List<String> columns_to_retrieve_item = new ArrayList<String>();
		columns_to_retrieve_item.add("I_TITLE");
		columns_to_retrieve_item.add("I_PUB_DATE");
		columns_to_retrieve_item.add("I_PUBLISHER");
		columns_to_retrieve_item.add("I_SUBJECT");
		columns_to_retrieve_item.add("I_IMAGE");
		columns_to_retrieve_item.add("I_DESC");
		columns_to_retrieve_item.add("I_COST");
		columns_to_retrieve_item.add("I_SRP");
		columns_to_retrieve_item.add("I_AVAIL");
		columns_to_retrieve_item.add("I_ISBN");
		columns_to_retrieve_item.add("I_PAGE");
		columns_to_retrieve_item.add("I_BACKING");
		columns_to_retrieve_item.add("I_DIMENSIONS");


		columns_to_retrieve_item.add("I_A_ID");

		Map<String, Object> item = getColumnMap(id + "", "item", columns_to_retrieve_item, READ_CONSISTENCY_LEVEL);

		List<String> columns_to_retrieve_author = new ArrayList<String>();
		columns_to_retrieve_author.add("A_FNAME");
		columns_to_retrieve_author.add("A_LNAME");
		int author_key = (Integer) item.get("I_A_ID");

		getColumnMap(author_key + "", "author", columns_to_retrieve_author, READ_CONSISTENCY_LEVEL);

	}

	public void AdminChange(int item_id) throws Exception {


		List<String> columns_to_retrieve_item = new ArrayList<String>();
		columns_to_retrieve_item.add("I_TITLE");
		columns_to_retrieve_item.add("I_PUB_DATE");
		columns_to_retrieve_item.add("I_PUBLISHER");
		columns_to_retrieve_item.add("I_SUBJECT");
		columns_to_retrieve_item.add("I_IMAGE");
		columns_to_retrieve_item.add("I_COST");
		columns_to_retrieve_item.add("I_DESC");
		columns_to_retrieve_item.add("I_SRP");
		columns_to_retrieve_item.add("I_AVAIL");
		columns_to_retrieve_item.add("I_ISBN");
		columns_to_retrieve_item.add("I_PAGE");
		columns_to_retrieve_item.add("I_BACKING");
		columns_to_retrieve_item.add("I_DIMENSIONS");
		columns_to_retrieve_item.add("I_A_ID");

		Map<String, Object> item = getColumnMap(item_id + "", "item", columns_to_retrieve_item, READ_CONSISTENCY_LEVEL);

		Date date = (Date) item.get("I_PUB_DATE");
		String subject = (String) item.get("I_SUBJECT");
		int author_id = (Integer) item.get("I_A_ID");
		String title = (String) item.get("I_TITLE");

		updateIndex(subject, date, item_id, title, author_id);

		Map<String, Map<String, Map<String, Object>>> orders = super_rangeQuery("orders", null, 10000);
		//	Map<String, Map<String, Map<String, Object>>> orders = modified_super_rangeQuery("orders", "order_info", null, 10000);


		Map<Integer, Integer> items_info = new TreeMap<Integer, Integer>();

		for (Map<String, Map<String, Object>> orders_info : orders.values()) {
			boolean found = false;
			TreeMap<Integer, Integer> bought_items = new TreeMap<Integer, Integer>();
			for (Map.Entry<String, Map<String, Object>> order_line : orders_info.entrySet()) {
				String super_column_name = order_line.getKey();
				if (!super_column_name.equals("order_info")) {
					Map<String, Object> columns = order_line.getValue();
					int i_id = (Integer) columns.get("OL_I_ID");
					if (i_id == item_id) {
						found = true;

					} else {
						int item_qty = (Integer) columns.get("OL_QTY");
						bought_items.put(i_id, item_qty);
					}
				}
			}

			if (found == true) {
				for (Integer i_id : bought_items.keySet()) {
					if (items_info.containsKey(i_id)) {
						int current_qty = items_info.get(i_id);
						items_info.put(i_id, (bought_items.get(i_id) + current_qty));
					} else {
						items_info.put(i_id, bought_items.get(i_id));
					}
				}
			}
		}


		Map top_sellers = reverseSortByValue(items_info);

		List<Integer> best = new ArrayList<Integer>();
		int num = 0;
		for (Iterator<Integer> it = top_sellers.keySet().iterator(); it.hasNext(); ) {
			int key = it.next();
			best.add(key);
			num++;
			if (num == 5)
				break;
		}

		if (num < 5) {
			for (int i = num; i < 5; i++) {
				best.add(random.nextInt(990)); //the items are form 0 to 1000 right?
			}

		}
		//  System.out.println("RE_"+top_sellers.toString());

		long timestamp = System.currentTimeMillis();

		List<Column> columns = new ArrayList<Column>();

		Column c_related1 = new Column();
		c_related1.setName("I_RELATED1".getBytes("UTF-8"));
		c_related1.setValue(BenchmarkUtil.getBytes(best.get(0)));
		c_related1.setTimestamp(timestamp);

		Column c_related2 = new Column();
		c_related2.setName("I_RELATED2".getBytes("UTF-8"));
		c_related2.setValue(BenchmarkUtil.getBytes(best.get(1)));
		c_related2.setTimestamp(timestamp);


		Column c_related3 = new Column();
		c_related3.setName("I_RELATED3".getBytes("UTF-8"));
		c_related3.setValue(BenchmarkUtil.getBytes(best.get(2)));
		c_related3.setTimestamp(timestamp);


		Column c_related4 = new Column();
		c_related4.setName("I_RELATED4".getBytes("UTF-8"));
		c_related4.setValue(BenchmarkUtil.getBytes(best.get(3)));
		c_related4.setTimestamp(timestamp);


		Column c_related5 = new Column();
		c_related5.setName("I_RELATED5".getBytes("UTF-8"));
		c_related5.setValue(BenchmarkUtil.getBytes(best.get(4)));
		c_related5.setTimestamp(timestamp);


//		Column c_related2 = new Column("I_RELATED2".getBytes(), BenchmarkUtil.getBytes(best.get(1)), timestamp);
//		Column c_related3 = new Column("I_RELATED3".getBytes(), BenchmarkUtil.getBytes(best.get(2)), timestamp);
//		Column c_related4 = new Column("I_RELATED4".getBytes(), BenchmarkUtil.getBytes(best.get(3)), timestamp);
//		Column c_related5 = new Column("I_RELATED5".getBytes(), BenchmarkUtil.getBytes(best.get(4)), timestamp);

		columns.add(c_related1);
		columns.add(c_related2);
		columns.add(c_related3);
		columns.add(c_related4);
		columns.add(c_related5);

		float I_COST = random.nextInt(100);
		String image = new String("img" + random.nextInt(1000) % 100 + "/image_" + random.nextInt(1000) + ".gif");
		Date new_date = new Date(System.currentTimeMillis());
		String thumb = image.replace("image", "thumb");

		Column c_cost = new Column();
		c_cost.setName("I_COST".getBytes("UTF-8"));
		c_cost.setValue(BenchmarkUtil.getBytes(I_COST));
		c_cost.setTimestamp(timestamp);

		Column c_image = new Column();
		c_image.setName("I_IMAGE".getBytes("UTF-8"));
		c_image.setValue(BenchmarkUtil.getBytes(image));
		c_image.setTimestamp(timestamp);

		Column c_thumb = new Column();
		c_thumb.setName("I_THUMBNAIL".getBytes("UTF-8"));
		c_thumb.setValue(BenchmarkUtil.getBytes(thumb));
		c_thumb.setTimestamp(timestamp);

		Column c_date = new Column();
		c_date.setName("I_PUB_DATE".getBytes("UTF-8"));
		c_date.setValue(BenchmarkUtil.getBytes(new_date));
		c_date.setTimestamp(timestamp);

		columns.add(c_cost);
		columns.add(c_image);
		columns.add(c_thumb);
		columns.add(c_date);

		batch_mutate_columns(item_id + "", "item", columns, WRITE_CONSISTENCY_LEVEL);
	}


	public void updateIndex(String subject, Date date, int item_id, String title, int author_key) throws Exception {

		List<String> columns_to_retrieve_author = new ArrayList<String>();
		columns_to_retrieve_author.add("A_FNAME");
		columns_to_retrieve_author.add("A_LNAME");

		Map<String, Object> author = getColumnMap(author_key + "", "author", columns_to_retrieve_author, READ_CONSISTENCY_LEVEL);

		//new index info
		Map<String, Object> index_values = new TreeMap<String, Object>();
		index_values.put("A_FNAME", author.get("A_FNAME"));
		index_values.put("A_LNAME", author.get("A_LNAME"));
		index_values.put("I_TITLE", title);

		//insert new info
		Date d = new Date(System.currentTimeMillis());
		Long new_time_stamp = Long.MAX_VALUE - d.getTime();
		String new_index_key = new_time_stamp + "." + item_id;
		index(subject, "item_subject_index", new_index_key, index_values);


		//delete old
		Long old_time_stamp = Long.MAX_VALUE - date.getTime();
		String old_index_key = old_time_stamp + "." + item_id;
		remove_super(subject, "item_subject_index", old_index_key);
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
		for (Iterator it = list.iterator(); it.hasNext(); ) {
			Map.Entry entry = (Map.Entry) it.next();
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}


}
