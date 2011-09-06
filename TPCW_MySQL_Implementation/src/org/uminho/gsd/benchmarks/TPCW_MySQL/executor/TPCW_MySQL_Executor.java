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

package org.uminho.gsd.benchmarks.TPCW_MySQL.executor;


import org.apache.log4j.Logger;
import org.uminho.gsd.benchmarks.benchmark.BenchmarkNodeID;
import org.uminho.gsd.benchmarks.dataStatistics.ResultHandler;
import org.uminho.gsd.benchmarks.generic.BuyingResult;
import org.uminho.gsd.benchmarks.generic.Constants;
import org.uminho.gsd.benchmarks.generic.entities.Address;
import org.uminho.gsd.benchmarks.generic.entities.Customer;
import org.uminho.gsd.benchmarks.helpers.BenchmarkUtil;
import org.uminho.gsd.benchmarks.helpers.SqlReader;
import org.uminho.gsd.benchmarks.helpers.TPM_counter;
import org.uminho.gsd.benchmarks.helpers.ThinkTime;
import org.uminho.gsd.benchmarks.interfaces.Entity;
import org.uminho.gsd.benchmarks.interfaces.KeyGenerator;
import org.uminho.gsd.benchmarks.interfaces.Workload.Operation;
import org.uminho.gsd.benchmarks.interfaces.Workload.WorkloadGeneratorInterface;
import org.uminho.gsd.benchmarks.interfaces.executor.DatabaseExecutorInterface;

import java.sql.*;
import java.sql.Date;
import java.util.*;

public class TPCW_MySQL_Executor implements DatabaseExecutorInterface {

	private Logger logger = Logger.getLogger(TPCW_MySQL_Executor.class);

	public static int TOP_SEARCH_LIMIT = 3000;
	public static int BEST_SELLERS_NUM = 10;


	/**
	 * Last used client - load balancing purposes *
	 */
	int last_writer = 0;
	int last_reader = 0;

	/**
	 * Database name*
	 */
	private String database_name;

	/**
	 * This client result logger*
	 */
	ResultHandler client_result_handler;

	/**
	 * Write connections*
	 */
	List<Connection> write_connections;

	/**
	 * Read connections*
	 */
	List<Connection> read_connections;

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
	 * GEnerator of ids
	 */
	KeyGenerator keyGenerator;

	/**
	 * Think time*
	 */
	private long simulatedDelay;


	private String user = "";
	private String password = "";


	/**
	 * Node id
	 */

	int node_id;

	/**
	 * The number of clients in onde node
	 */
	int one_node_clients;


	private Map<String, String> read_paths;
	private Map<String, String> write_paths;


	int addr_aux_id = 0;

	int num_operations = 0;

	/**
	 * Tables primary keys
	 */
	private Map<String, List<String>> primaryKeys;


	private boolean has_shoppingCart = false;

	/**
	 * Benchmark queries read from file.
	 */
	Map<String, String> benchmark_queries;


	private TPM_counter counter;

	/**
	 * TPCW VARIABLES*
	 */
	private String[] credit_cards = {"VISA", "MASTERCARD", "DISCOVER", "AMEX", "DINERS"};
	private String[] ship_types = {"AIR", "UPS", "FEDEX", "SHIP", "COURIER", "MAIL"};
	private String[] status_types = {"PROCESSING", "SHIPPED", "PENDING", "DENIED"};

	private Random random = new Random();


	public TPCW_MySQL_Executor(String databaseName, String user, String password, Map<String, String> read_paths, Map<String, String> write_paths, int number_clients, TPM_counter tpm_counter) {


		this.one_node_clients = number_clients;
		this.database_name = databaseName;
		read_connections = new ArrayList<Connection>();
		write_connections = new ArrayList<Connection>();
		this.read_paths = read_paths;
		this.write_paths = write_paths;
		this.user = user;
		this.password = password;
		this.counter = tpm_counter;

		//	createDatabase();

		TreeMap<String, Connection> connections = new TreeMap<String, Connection>();

		for (String hostname : read_paths.keySet()) {


			String connection_url = hostname + ":" + read_paths.get(hostname);

			if (!connections.containsKey(connection_url)) {

				try {

					String url = "jdbc:mysql://" + connection_url + "/" + database_name;
					try {

						Class.forName("com.mysql.jdbc.Driver").newInstance();

					} catch (ClassNotFoundException e) {
						e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
						logger.error("[Error]: Cant find MySQL JDBC driver");
					} catch (InstantiationException e) {
						e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
					} catch (IllegalAccessException e) {
						e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
					}

					Connection connection =
							DriverManager.getConnection(
									url, user, password);

					connections.put(connection_url, connection);
				} catch (SQLException e) {
					e.printStackTrace();
					logger.error("[ERROR]: Connection error", e);
				}


			}

			if (connections.containsKey(connection_url)) {
				read_connections.add(connections.get(connection_url));
			}
		}

		for (String hostname : write_paths.keySet()) {


			String connection_url = hostname + ":" + write_paths.get(hostname);

			if (!connections.containsKey(connection_url)) {

				try {

					String url = "jdbc:mysql://" + connection_url + "/" + database_name;

					Connection connection =
							DriverManager.getConnection(
									url, user, password);

					//connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

					connections.put(connection_url, connection);
				} catch (SQLException e) {
					e.printStackTrace();
					logger.error("[ERROR]: Connection error", e);
				}


			}

			if (connections.containsKey(connection_url)) {
				write_connections.add(connections.get(connection_url));
			}
		}

		benchmark_queries = SqlReader.parse("conf/DataStore/sql-mysql.properties");
		getTableKeys();

	}

	public void start(WorkloadGeneratorInterface workload, BenchmarkNodeID nodeId, int operation_number, ResultHandler handler) {

		this.node_id = nodeId.getId();

		client_result_handler = handler;

		this.num_operations = operation_number;
		int r = random.nextInt(100);


		for (int operation = 0; operation < operation_number; operation++) {

			long g_init_time = System.currentTimeMillis();

			try {
				long init_time = System.currentTimeMillis();

				Operation op = workload.getNextOperation();
				execute(op);
				long end_time = System.currentTimeMillis();
				client_result_handler.logResult(op.getOperation(), (end_time - init_time));


				simulatedDelay = ThinkTime.getThinkTime();

				if (simulatedDelay > 0) {
					Thread.sleep(simulatedDelay);
				}

			} catch (NoSuchFieldException e) {
				System.out.println("[ERROR:] THIS OPERATION DOES NOT EXIST: " + e.getMessage());
			} catch (InterruptedException e) {
				System.out.println("[ERROR:] THINK TIME AFTER METHOD EXECUTION INTERRUPTED: " + e.getMessage());
			} catch (Exception e) {
				e.printStackTrace();
			}
			long end_time = System.currentTimeMillis();
			counter.increment();
			//client_result_handler.logResult("OPERATIONS", (end_time - g_init_time));

		}

		client_result_handler.getResulSet().put("bought", partialBought);
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

		if (method_name.equalsIgnoreCase("CREATE_TABLES")) {
			createTables();
		} else if (method_name.equalsIgnoreCase("CREATE_INDEXES")) {
			addIndexes();
		} else if (method_name.equalsIgnoreCase("REMOVE_CREATE_DATABASE")) {
			remove_and_create_Database();
		} else if (method_name.equalsIgnoreCase("GET_STOCK_AND_PRODUCTS")) {
			ArrayList<String> fields = new ArrayList<String>();
			fields.add("i_title");
			fields.add("i_stock");
			Map<String, Map<String, Object>> items_info = rangeQuery("ITEM", fields, -1);
			op.setResult(items_info);


		} else if (method_name.equalsIgnoreCase("Get_Stock_And_Products_after_increment")) {

			ArrayList<String> fields = new ArrayList<String>();
			int stock = (Integer) op.getParameter("STOCK");
			setItemStocks(stock);

			fields.add("i_stock");
			Map<String, Map<String, Object>> items_info = rangeQuery("ITEM", fields, -1);
			op.setResult(items_info);


		} else if (method_name.equalsIgnoreCase("GET_ITEM_STOCK")) {

			String item_id = (String) op.getParameters().get("ITEM_ID");

			Object o = read(item_id, "ITEM", "I_STOCK", null);
			int stock = -1;
			if (o != null) {
				stock = (Integer) o;
			}
			op.setResult(stock);

		} else if (method_name.equalsIgnoreCase("ADD_TO_CART")) {

			int cart = getIDfromString((String) op.getParameter("CART_ID"));
			String item_id = (String) op.getParameter("ITEM_ID");
			int qty = (Integer) op.getParameter("QTY");

			addToCart(cart, item_id, qty);

		} else if (method_name.equalsIgnoreCase("BUY_CART")) {

			bought_carts++;
			int cart_id = getIDfromString((String) op.getParameter("CART_ID"));
			Object c = null;
			if (op.getParameters().containsKey("COSTUMER")) {
				c = op.getParameter("COSTUMER");
			}
			buyCart(cart_id, c);


		} else if (method_name.equalsIgnoreCase("TOP_SELLERS")) {

			getTopSellers(BEST_SELLERS_NUM);

		} else if (method_name.equalsIgnoreCase("SEARCH_ITEM_BY_SUBJECT")) {

			String subject = (String) op.getParameter("SUBJECT");
			readItemWhere("item", "I_SUBJECT", subject);

		} else if (method_name.equalsIgnoreCase("SEARCH_ITEM_BY_TITLE")) {
			String subject = (String) op.getParameter("TITLE");
			readItemWhere("item", "I_TITLE", subject);

		} else if (method_name.equalsIgnoreCase("SEARCH_ITEM_BY_AUTHOR")) {

			String name = (String) op.getParameter("AUTHOR");
			readItemWhere("item", "A_LNAME", name);

		} else if (method_name.equalsIgnoreCase("NEW_PRODUCTS")) {

			String subject = (String) op.getParameter("SUBJECT");
			getNewProducts(subject);

		} else if (method_name.equalsIgnoreCase("GET_BENCHMARK_RESULTS")) {
			op.setResult(getResults());

		} else if (method_name.equalsIgnoreCase("DELETE_ADDR_CUST")) {
			int num_a = (Integer) op.getParameter("NUM_A");
			int num_c = (Integer) op.getParameter("NUM_C");


			PreparedStatement delete_addr = null;
			try {
				delete_addr = getWriteConnection().prepareStatement("DELETE FROM ADDRESS WHERE ADDR_ID >?");
				// Set parameter
				delete_addr.setInt(1, num_a);
				delete_addr.execute();

				delete_addr = getWriteConnection().prepareStatement("DELETE FROM CUSTOMER WHERE C_ID >?");
				// Set parameter
				delete_addr.setInt(1, num_c);
				delete_addr.execute();


			} catch (SQLException e) {
				e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
			}

		} else if (method_name.equalsIgnoreCase("OP_HOME")) {

			int costumer = (Integer) op.getParameter("COSTUMER");
			int item_id = (Integer) op.getParameter("ITEM");
			HomeOperation(costumer, item_id);
		} else if (method_name.equalsIgnoreCase("OP_SHOPPING_CART")) {


			boolean create = (Boolean) op.getParameter("CREATE");

			int item_id = (Integer) op.getParameter("ITEM");

			String cart_id = (String) op.getParameter("CART");

			int id = getIDfromString(cart_id);

			shoppingCartInteraction(item_id, create, id);

		} else if (method_name.equalsIgnoreCase("OP_REGISTER")) {

			String customer = (String) op.getParameter("CUSTOMER");
			int id = getIDfromString(customer, Constants.NUM_CUSTOMERS);
			int process_id = getProcessId(customer);
			CustomerRegistration(process_id, id);


		} else if (method_name.equalsIgnoreCase("OP_LOGIN")) {

			String customer = (String) op.getParameter("CUSTOMER");
			int id = Integer.parseInt(customer);

			refreshSession(id);

		} else if (method_name.equalsIgnoreCase("OP_BUY_REQUEST")) {

			String id = (String) op.getParameter("CART");
			int cart_id = getIDfromString(id);


			BuyRequest(cart_id);

		} else if (method_name.equalsIgnoreCase("OP_BUY_CONFIRM")) {

			String cart = (String) op.getParameter("CART");
			String custumer = (String) op.getParameter("CUSTOMER");

			int cust_id = Integer.parseInt(custumer.trim());

			int process_id = getProcessId(cart);
			int cart_id = getIDfromString(cart);

			BuyComfirm(cust_id, process_id, cart_id);

		} else if (method_name.equalsIgnoreCase("OP_ORDER_INQUIRY")) {
			String customer = (String) op.getParameter("CUSTOMER");
			int customer_id = Integer.parseInt(customer);


			OrderInquiry(customer_id);


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

	private void setItemStocks(int initial_stock) {

		ArrayList<String> fields = new ArrayList<String>();
		fields.add("i_stock");
		Map<String, Map<String, Object>> items_info = rangeQuery("ITEM", fields, -1);


		for (String key : items_info.keySet()) {
			update(key, "ITEM", "i_stock", initial_stock, null);
		}

		//sleep to garantee the execution of all request in a low isolation level
		try {
			System.out.println("Sleeping after item stock reposition...");
			Thread.sleep(60000);
		} catch (InterruptedException e) {

		}
	}


	public Map<String, Map<String, Map<String, Object>>> getResults() {

		String column_family = "RESULTS";


		Map<String, Map<String, Map<String, Object>>> results = new TreeMap<String, Map<String, Map<String, Object>>>();


		List<Map<String, Object>> result_info = getFieldList("RESULTS", -1);

		//I think its a client_id  :-)
		for (Map<String, Object> columns : result_info) {

			String key_name = columns.get("client_id").toString();

			if (!results.containsKey(key_name)) {
				results.put(key_name, new TreeMap<String, Map<String, Object>>());
			}

			int item_id = (Integer) columns.get("item_id");	//for each item

			if (!results.get(key_name).containsKey(item_id + "")) {
				results.get(key_name).put(item_id + "", new TreeMap<String, Object>());
			}

			for (String column_name : columns.keySet()) {
				if (column_name.equals("item_id")) {
					continue;
				}
				results.get(key_name).get(item_id + "").put(column_name, columns.get(column_name));
			}
		}

		return results;

	}

	private int getProcessId(String id) {
		String[] parts = id.trim().split("\\.");
		int process_id = Integer.parseInt(parts[1]);
		return process_id;
	}

	private int getIDfromString(String id) {
		String[] parts = id.trim().split("\\.");

		int node_id = Integer.parseInt(parts[0]);
		int process_id = Integer.parseInt(parts[1]);


		int length_proc = (this.one_node_clients + "").length();


		int id_id = Integer.parseInt(parts[2]);


		//  int length_id = parts[2].length();
		int length_id = (num_operations + "").length();

		int final_id = (int) (node_id * Math.pow(10, (length_id + length_proc + 0d))) + (int) (process_id * Math.pow(10, (length_id + 0d))) + id_id;

		return final_id;
	}

	private int getIDfromString(String id, int num) {
		String[] parts = id.trim().split("\\.");

		int node_id = Integer.parseInt(parts[0]);
		int process_id = Integer.parseInt(parts[1]);

		int length_proc = (this.one_node_clients + "").length();

		int id_id = Integer.parseInt(parts[2]);

		//  int length_id = parts[2].length();
		int length_id = ((num + num_operations) + "").length();

		int final_id = (int) (node_id * Math.pow(10, (length_id + length_proc + 0d))) + (int) (process_id * Math.pow(10, (length_id + 0d))) + (id_id + num + 1);

		return final_id;
	}

	private int getIDfromValues(int node_id, int process_id, int id_id, int num) {

		int length_proc = (this.one_node_clients + "").length();

		//  int length_id = parts[2].length();
		int length_id = ((num + num_operations) + "").length();

		int final_id = (int) (node_id * Math.pow(10, (length_id + length_proc + 0d))) + (int) (process_id * Math.pow(10, (length_id + 0d))) + (id_id + num + 1);

		return final_id;
	}

	public void getTableKeys() {

		primaryKeys = new TreeMap<String, List<String>>();

		PreparedStatement statement = null;
		try {
			statement = getWriteConnection().prepareStatement("SELECT TABLE_NAME,COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA= ? AND COLUMN_KEY='PRI'");

			statement.setString(1, database_name);
			ResultSet set = statement.executeQuery();
			while (set.next()) {
				// Get the data from the row using the column name

				String table_name = set.getString("TABLE_NAME").toUpperCase();
				String column_name = set.getString("COLUMN_NAME");

				if (primaryKeys.containsKey(table_name)) {
					primaryKeys.get(table_name).add(column_name);

				} else {
					ArrayList<String> table_keys = new ArrayList<String>();
					table_keys.add(column_name);
					primaryKeys.put(table_name, table_keys);
				}
			}
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("[PRIMARY KEY RETREIVEL]:", e);
		}
		logger.debug("[PRIMARY KEYS]: " + primaryKeys.toString());
	}

	public Connection getWriteConnection() {

		boolean openClient = false;
		Connection cl = null;

		while (!openClient) {   //until there is one open

			if (!write_connections.isEmpty()) {   //if none, then null...
				cl = write_connections.get(last_writer);
				last_writer++;
				last_writer = last_writer >= write_connections.size() ? 0 : last_writer;
				openClient = true;

			} else {
				openClient = true;
			}
		}

		return cl;

	}

	public Connection getReadConnection() {

		boolean openClient = false;
		Connection cl = null;

		while (!openClient) {   //until there is one open

			if (!read_connections.isEmpty()) {   //if none, then null...
				cl = read_connections.get(last_reader);
				last_reader++;
				last_reader = last_reader >= read_connections.size() ? 0 : last_reader;
				openClient = true;

			} else {
				openClient = true;
			}
		}

		return cl;
	}

	public Connection getRawConnection() {


		Connection connection = null;

		for (String hostname : write_paths.keySet()) {


			String connection_url = hostname + ":" + write_paths.get(hostname);


			connection = null;
			try {

				String url = "jdbc:mysql://" + connection_url;

				connection = DriverManager.getConnection(
						url, user, password);

			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("[ERROR]: Connection error", e);
			}

			break;

		}

		return connection;
	}

	/**
	 * ************
	 * TPCW methods
	 * ************
	 */


	private void getTopSellers(int best_sellers_num) {

		try {
			PreparedStatement statement = getReadConnection().prepareStatement(benchmark_queries.get("@sql.getBestSellers"));
			statement.setInt(1, best_sellers_num);
			statement.execute();
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("[TOP SELLERS QUERY]: " + e.getMessage());
		}
	}


	private void addToCart(int cart, String item_id, int qty) {

		if (!has_shoppingCart) {
			try {
				PreparedStatement insert_new_cart = getWriteConnection().prepareStatement(benchmark_queries.get("@sql.createEmptyCart"));

				insert_new_cart.setInt(1, cart);
				insert_new_cart.execute();

			} catch (java.lang.Exception ex) {
				ex.printStackTrace();
				logger.error("[ERROR - NEW CART INSERTION]:" + ex.getMessage());
			}
			has_shoppingCart = true;
		}
		try {
			PreparedStatement find_entry = getReadConnection().prepareStatement(benchmark_queries.get("@sql.addItem"));

			int item_int_id = Integer.parseInt(item_id.trim());
			// Set parameter
			find_entry.setInt(1, cart);
			find_entry.setInt(2, item_int_id);
			ResultSet rs = find_entry.executeQuery();


			// Results
			if (rs.next()) {
				//The shopping cart already has an item with this id.
				int quantity = rs.getInt("scl_qty");
				quantity += qty;
				PreparedStatement update_qty = getWriteConnection().prepareStatement(benchmark_queries.get("@sql.addItem.update"));
				update_qty.setInt(1, quantity);
				update_qty.setInt(2, cart);
				update_qty.setInt(3, item_int_id);
				update_qty.executeUpdate();
				update_qty.close();
			} else {

				//Add new item to shopping cart
				Connection con = getWriteConnection();
				con.setAutoCommit(false);
				PreparedStatement put_line = con.prepareStatement(benchmark_queries.get("@sql.addItem.put"));

				put_line.setInt(1, cart);
				put_line.setInt(2, qty);
				put_line.setInt(3, item_int_id);
				put_line.setFloat(4, 1);
				put_line.setDouble(5, 1);
				put_line.setString(6, "");
				put_line.setString(7, "");

				put_line.executeUpdate();
				con.commit();
				put_line.close();
				//  con.close();
			}

			rs.close();
			find_entry.close();


		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}
	}

	private void readItemWhere(String table, String field, String subject) {
		try {

			if (field.equals("A_LNAME")) {

				PreparedStatement search = getReadConnection().prepareStatement(benchmark_queries.get("@sql.doAuthorSearch"));
				search.setString(1, subject + "%");
				ResultSet rs = search.executeQuery();
			} else if (field.equals("I_TITLE")) {

				PreparedStatement search = getReadConnection().prepareStatement(benchmark_queries.get("@sql.doTitleSearch"));
				search.setString(1, subject + "%");
				ResultSet rs = search.executeQuery();

			} else if (field.equals("I_SUBJECT")) {

				PreparedStatement search = getReadConnection().prepareStatement(benchmark_queries.get("@sql.doSubjectSearch"));
				search.setString(1, subject + "%");
				ResultSet rs = search.executeQuery();
			}

		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("[ERROR - ITEM SEARCH BY " + field + "]:" + e.getMessage());

		}

	}

	private void buyCart(int cart_id, Object c) {
		has_shoppingCart = false;

		//read cart
		try {
			Connection con = getWriteConnection();
			//		con.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			con.setAutoCommit(false);
			PreparedStatement get_cart = con.prepareStatement(benchmark_queries.get("@sql.getCartLines"));
			get_cart.setInt(1, cart_id);
			ResultSet rs = get_cart.executeQuery();

			//change stock
			while (rs.next()) {
				int item = rs.getInt("SCL_I_ID");
				int qty_read = rs.getInt("SCL_QTY");

				//       for (String item : cart.keySet()) {


				BuyingResult result = buyItem(item, qty_read, con);
				client_result_handler.countEvent("BUYING_COUNTERS", result.name(), 1);


				if (result.equals(BuyingResult.BOUGHT)) {
					bought_qty += qty_read;
					bought_actions++;
					if (!partialBought.containsKey(item + "")) {
						partialBought.put(item + "", qty_read);

					} else {
						int bought = partialBought.get(item + "");
						partialBought.put(item + "", (qty_read + bought));
					}

				}
			}

			rs.close();
			con.commit();
			get_cart.close();
			//  con.close();

		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}


		//build order


	}

	private BuyingResult buyItem(int item_id, Integer qty, Connection con) {

		long init_time = System.currentTimeMillis();
		BuyingResult result = BuyCartItem(item_id, qty, con);
		long end_time = System.currentTimeMillis();
		client_result_handler.logResult("BUY_ITEM", (end_time - init_time));
		List<Object> buying_information = new LinkedList<Object>();
		buying_information.add(qty);
		buying_information.add(init_time);
		buying_information.add(end_time);
		client_result_handler.record_unstructured_data("BOUGHT_ITEMS_TIMELINE", item_id + "", buying_information);
		return result;

	}

	public BuyingResult BuyCartItem(int item, int qty, Connection con) {

		try {

			Object o = getStock(con, item);

			// con.close();


			if (o == null) {
				con.commit();
				return BuyingResult.DOES_NOT_EXIST;

			}
			int stock = (Integer) o;
			if ((stock - qty) >= 0) {   //if stock is sufficient

				if (stock - qty == 0) {
					zeros++;
				}

				stock -= qty;
				//            con = getWriteConnection();
				setStock(con, item, stock);

				//               con.commit();
				// con.setAutoCommit(true);
			} else {
				return BuyingResult.NOT_AVAILABLE;
			}
			return BuyingResult.BOUGHT;

		} catch (Exception e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}

		return BuyingResult.CANT_COMFIRM;
	}


	public void getNewProducts(String subject) {
		try {
			// Prepare SQL
			PreparedStatement new_products = getReadConnection().prepareStatement(benchmark_queries.get("@sql.getNewProducts"));

			// Set parameter
			new_products.setString(1, subject);
			ResultSet rs = new_products.executeQuery();

			rs.close();
			new_products.close();

		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}

	}


	/**
	 * ************
	 * CRUD methods
	 * ************
	 */


	public PreparedStatement insertStatementValues(PreparedStatement statement, Map<String, Object> values) {

		int i = 1;
		for (String value_name : values.keySet()) {

			Object value = values.get(value_name);

			statement = insertStatmentValue(statement, i, value);

			i++;
		}
		return statement;

	}

	public PreparedStatement insertStatmentValue(PreparedStatement statement, int index, Object value) {

		try {


			if (value instanceof Integer) {

				statement.setInt(index, (Integer) value);

			} else if (value instanceof Float) {

				statement.setFloat(index, (Float) value);

			} else if (value instanceof Double) {

				statement.setDouble(index, (Double) value);

			} else if (value instanceof Long) {

				statement.setLong(index, (Long) value);

			} else if (value instanceof String) {

				statement.setString(index, (String) value);

			} else if (value instanceof Short) {

				statement.setShort(index, (Short) value);

			} else if (value instanceof Byte) {

				statement.setByte(index, (Byte) value);

			} else if (value instanceof Timestamp) {

				statement.setTimestamp(index, (Timestamp) value);

			} else if (value instanceof Date) {

				statement.setDate(index, (Date) value);
			} else {

				if (value == null) {
					logger.warn("[INSERT:] OBJECT VALUE IS NULL: ");

				} else {
					logger.debug("[INSERT:] OBJECT CLASS NOT TREATED: " + value.getClass().getName());
				}


				statement.setObject(index, value);
			}
			index++;

		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("[INSERTING VALUES ON STATMENT]:", e);
		}

		return statement;
	}

	public Object insert(String key, String path, Entity value) {

		path = path.toUpperCase();

		String column_names = "( " + value.getKeyName().toLowerCase();
		String values = "( " + key;

		TreeMap<String, Object> values_to_insert = value.getValuesToInsert();

		for (String column_name : values_to_insert.keySet()) {
			column_names += "," + column_name.toLowerCase();
			values += ",?";
		}

		column_names += ")";
		values += ")";
		PreparedStatement statement = null;
		try {
			statement = getWriteConnection().prepareStatement("INSERT INTO " + path + " " + column_names + " VALUES " + values);
			insertStatementValues(statement, values_to_insert);
			logger.debug("[INSERT STATEMENT]:" + statement.toString());

			statement.execute();
			statement.close();

		} catch (SQLException e) {
			logger.error("[INSERT]: " + statement.toString());
			e.printStackTrace();
		}


		return null;

	}

	public void remove(String key, String path, String column) {

		path = path.toUpperCase();

		String key_name = primaryKeys.get(path).get(0);

		try {
			PreparedStatement statement = null;
			if (column != null) {
				statement = getWriteConnection().prepareStatement(" DELETE " + column + " FROM " + path + " WHERE " + key_name + "=" + key);
			} else {
				statement = getWriteConnection().prepareStatement(" DELETE * FROM " + path + " WHERE " + key_name + "=" + key);
			}
			logger.debug("[DELETE STATEMENT]:" + statement.toString());

			statement.execute();
			statement.close();

		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("[REMOVE]: ", e);
		}

	}

	public void update(String key, String path, String column, Object value, String superfield) {

		path = path.toUpperCase();

		String key_name = primaryKeys.get(path).get(0);

		try {
			PreparedStatement statement = getWriteConnection().prepareStatement(" UPDATE " + path + " SET " + column + "= ? WHERE " + key_name + "=" + key);
			statement = insertStatmentValue(statement, 1, value);
			logger.debug("[UPDATE STATEMENT]:" + statement.toString());
			statement.execute();
			statement.close();

		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("[UPDATE]: ", e);
		}
	}

	public Object read(String key, String path, String column, String superfield) {

		path = path.toUpperCase();

		String key_name = primaryKeys.get(path).get(0);
		Object result = null;
		try {
			PreparedStatement statement = getReadConnection().prepareStatement(" SELECT " + column + " FROM " + path + " WHERE " + key_name + "=" + key);
			logger.debug("[READ STATEMENT]:" + statement.toString());

			ResultSet set = statement.executeQuery();
			while (set.next()) {
				result = set.getObject(column);
			}
			statement.close();

		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("[READ]: ", e);
		}

		return result;

	}

	public Map<String, Map<String, Object>> rangeQuery(String table, List<String> fields, int limit) {

		table = table.toUpperCase();

		String key_name = primaryKeys.get(table).get(0);
		Map<String, Map<String, Object>> result = new TreeMap<String, Map<String, Object>>();
		try {
			String limit_parameter = (limit < 0) ? "" : " LIMIT " + limit;

			PreparedStatement statement = getReadConnection().prepareStatement(" SELECT * FROM " + table + limit_parameter);
			logger.debug("[READ STATEMENT]:" + statement.toString());

			ResultSet set = statement.executeQuery();

			ResultSetMetaData metaData = set.getMetaData();
			int column_count = metaData.getColumnCount();
			String column_name = "";
			String key = "";

			while (set.next()) {

				TreeMap<String, Object> values = new TreeMap<String, Object>();

				key = set.getObject(key_name).toString();

				for (int i = 1; i < column_count; i++) {
					column_name = metaData.getColumnName(i);
					Object column_value = set.getObject(column_name);
					values.put(column_name, column_value);
				}
				result.put(key, values);
			}

			statement.close();

		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("[READ]: ", e);
		}
		return result;


	}

	public List<Map<String, Object>> getFieldList(String table, int limit) {

		table = table.toUpperCase();

		String key_name = primaryKeys.get(table.toUpperCase()).get(0);
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		try {
			String limit_parameter = (limit < 0) ? "" : " LIMIT " + limit;

			PreparedStatement statement = getReadConnection().prepareStatement(" SELECT * FROM " + table + limit_parameter);
			logger.debug("[READ STATEMENT]:" + statement.toString());

			ResultSet set = statement.executeQuery();

			ResultSetMetaData metaData = set.getMetaData();
			int column_count = metaData.getColumnCount();
			column_count++;
			String column_name = "";
			String key = "";

			while (set.next()) {

				TreeMap<String, Object> values = new TreeMap<String, Object>();

				key = set.getObject(key_name).toString();

				for (int i = 1; i < column_count; i++) {
					column_name = metaData.getColumnName(i);
					Object column_value = set.getObject(column_name);
					values.put(column_name, column_value);
				}
				result.add(values);
			}

			statement.close();

		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("[READ]: ", e);
		}
		return result;


	}

	public void truncate(String path) {

		System.out.println("TRUNCATE: " + path);
		String table = path.toUpperCase();
		try {
			PreparedStatement statement = getWriteConnection().prepareStatement("TRUNCATE TABLE " + database_name + "." + table);
			logger.debug("[TRUNCATE]:" + statement.toString());
			statement.execute();

		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("[ERROR TRUNCATING TABLE]: " + path);
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
		TreeMap<String, String> map = new TreeMap<String, String>();
		map.put("read paths", read_paths.toString());
		map.put("write paths", write_paths.toString());


		return map;  //To change body of implemented methods use File | Settings | File Templates.
	}


	/**
	 * ************
	 * TPCW operations
	 * ************
	 */

	public void HomeOperation(int costumer, int item) {

		Connection con = getReadConnection();
		try {

			//  System.out.println(benchmark_queries.get("@sql.getName"));
			//    System.out.println(getReadConnection());
			con.setAutoCommit(false);
			PreparedStatement name = con.prepareStatement(benchmark_queries.get("@sql.getName"));

			name.setInt(1, costumer);
			ResultSet rs = name.executeQuery();
			con.commit();

			while (rs.next()) {
				//  System.out.println("RS1:" + rs.getObject(1));
			}


			rs.close();
			name.close();

		} catch (SQLException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}

		try {
			PreparedStatement related = con.prepareStatement(benchmark_queries.get("@sql.getRelated"));

			related.setInt(1, item);
			ResultSet rs = related.executeQuery();
			con.commit();
			//  System.out.println("RS2:" + rs.first());
			rs.close();
			related.close();

		} catch (SQLException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}


	}


	public int shoppingCartInteraction(int item, boolean create, int SHOPPING_ID) {

		Connection con = getWriteConnection();

		if (create) {
			try {

				con.setAutoCommit(false);


				PreparedStatement insert_cart = con.prepareStatement
						(benchmark_queries.get("@sql.createEmptyCart"));
				insert_cart.setInt(1, SHOPPING_ID);
				insert_cart.executeUpdate();


				insert_cart.close();
				con.commit();


			} catch (java.lang.Exception ex) {
				ex.printStackTrace();
			}


		}


		try {
			// Prepare SQL

			con = getWriteConnection();
			con.setAutoCommit(false);

			PreparedStatement find_info = con.prepareStatement(benchmark_queries.get("@sql.getItemInfo"));
			find_info.setInt(1, item);
			ResultSet rset = find_info.executeQuery();

			rset.next();
			float cost = rset.getFloat("I_COST");
			double srp = rset.getDouble("I_SRP");
			String title = rset.getString("I_TITLE");
			String back = rset.getString("I_BACKING");

			PreparedStatement find_entry = con.prepareStatement(benchmark_queries.get("@sql.addItem"));

			// Set parameter
			find_entry.setInt(1, SHOPPING_ID);
			find_entry.setInt(2, item);
			ResultSet rs = find_entry.executeQuery();
			con.commit();


			// Results
			if (rs.next()) {
				//The shopping cart id, item pair were already in the table
				int currqty = rs.getInt("scl_qty");
				currqty += 1;
				PreparedStatement update_qty = con.prepareStatement(benchmark_queries.get("@sql.addItem.update"));
				update_qty.setInt(1, currqty);
				update_qty.setInt(2, SHOPPING_ID);
				update_qty.setInt(3, item);
				update_qty.executeUpdate();
				con.commit();
				update_qty.close();
			} else {//We need to add a new row to the table.

				//Stick the item info in a new shopping_cart_line
				PreparedStatement put_line = con.prepareStatement(benchmark_queries.get("@sql.addItem.put"));
				put_line.setInt(1, SHOPPING_ID);
				put_line.setInt(2, 1);
				put_line.setInt(3, item);
				put_line.setFloat(4, cost);
				put_line.setDouble(5, srp);
				put_line.setString(6, title);
				put_line.setString(7, back);
				put_line.executeUpdate();
				con.commit();
				put_line.close();
			}
			rs.close();
			find_entry.close();
		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}


		return SHOPPING_ID;
	}


	public void CustomerRegistration(int process_id, int id) {

		try {
			// Get largest customer ID already in use.
			Connection con = getWriteConnection();
			con.setAutoCommit(false);

			Customer cust = createCostumer();


			// FIXME - Use SQL CURRENT_TIME to do this
			cust.setLastLogin(new Date(System.currentTimeMillis()));
			cust.setSince(new Date(System.currentTimeMillis()));

			cust.setLogin(new Timestamp(System.currentTimeMillis()));

			cust.setExpiration(new Timestamp(System.currentTimeMillis() + 7200000));// 2 hours  in   milliseconds
			PreparedStatement insert_customer_row = con.prepareStatement(benchmark_queries.get("@sql.createNewCustomer"));
			insert_customer_row.setString(4, cust.getC_FNAME());
			insert_customer_row.setString(5, cust.getC_LNAME());
			insert_customer_row.setString(7, cust.getC_PHONE());
			insert_customer_row.setString(8, cust.getC_EMAIL());
			insert_customer_row.setDate(9, new java.sql.Date(cust.getSince().getTime()));
			insert_customer_row.setDate(10, new java.sql.Date(cust.getLastLogin().getTime()));
			insert_customer_row.setDate(11, new java.sql.Date(cust.getLogin().getTime()));
			insert_customer_row.setDate(12, new java.sql.Date(cust.getExpiration().getTime()));
			insert_customer_row.setDouble(13, cust.getC_DISCOUNT());
			insert_customer_row.setDouble(14, cust.getBalance());
			insert_customer_row.setDouble(15, cust.getYtd_pmt());
			insert_customer_row.setDate(16, new java.sql.Date(cust.getBirthdate().getTime()));
			insert_customer_row.setString(17, cust.getData());

			Address address = generateAddress();
			int a_id = getIDfromValues(node_id, process_id, addr_aux_id, Constants.NUM_ADDRESSES);
			addr_aux_id++;

			cust.setAddress(enterAddress(con, a_id,
					address.getStreet1(),
					address.getStreet2(),
					address.getCity(),
					address.getState(),
					address.getZip(),
					address.getCountry_id())
			);


			cust.setC_id(id);//Is 1 the correct index?

			cust.setC_UNAME(BenchmarkUtil.DigSyl(cust.getC_id(), 0));
			cust.setC_PASSWD(cust.getC_UNAME().toLowerCase());


			insert_customer_row.setInt(1, cust.getC_id());
			insert_customer_row.setString(2, cust.getC_UNAME());
			insert_customer_row.setString(3, cust.getC_PASSWD());
			insert_customer_row.setInt(6, cust.getAddress());
			insert_customer_row.executeUpdate();
			con.commit();
			insert_customer_row.close();


		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}
	}

	private Customer createCostumer() {

		String name = (BenchmarkUtil.getRandomAString(8, 13) + " " + BenchmarkUtil.getRandomAString(8, 15));
		String[] names = name.split(" ");
		Random r = new Random();
		int random_int = r.nextInt(1000);

		String pass = names[0].charAt(0) + names[1].charAt(0) + "" + random_int;
		//  insert(pass, key, "Customer", "C_PASSWD", writeCon);

		String first_name = names[0];
		//  insert(first_name, key, "Customer", "C_FNAME", writeCon);

		String last_name = names[1];
		//  insert(last_name, key, "Customer", "C_LNAME", writeCon);

		int phone = r.nextInt(999999999 - 100000000) + 100000000;
		//  insert(phone, key, "Customer", "C_PHONE", writeCon);

		String email = first_name + "@" + BenchmarkUtil.getRandomAString(2, 9) + ".com";
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


		//insert(address.getAddr_id(), key, "Customer", "C_ADDR_ID", writeCon);

		Customer c = new Customer(-1, "", pass, last_name, first_name, phone + "", email, C_SINCE, C_LAST_LOGIN, C_LOGIN, C_EXPIRATION, C_BALANCE, C_YTD_PMT, C_BIRTHDATE, C_DATA, discount, -1);

		return c;
	}


	public Address generateAddress() {


		String ADDR_STREET1, ADDR_STREET2, ADDR_CITY, ADDR_STATE;
		String ADDR_ZIP;
		int country_id;

		ADDR_STREET1 = "street" + BenchmarkUtil.getRandomAString(10, 30);


		ADDR_STREET2 = "street" + BenchmarkUtil.getRandomAString(10, 30);
		ADDR_CITY = BenchmarkUtil.getRandomAString(4, 30);
		ADDR_STATE = BenchmarkUtil.getRandomAString(2, 20);
		ADDR_ZIP = BenchmarkUtil.getRandomAString(5, 10);
		country_id = BenchmarkUtil.getRandomInt(0, 91);

		Address address = new Address(0, ADDR_STREET1, ADDR_STREET2, ADDR_CITY,
				ADDR_STATE, ADDR_ZIP, country_id);
		return address;

	}

	public int enterAddress(Connection con, int id,  // Do we need to do this as part of a transaction?
							String street1, String street2,
							String city, String state,
							String zip, int country) {
		// returns the address id of the specified address.  Adds a
		// new address to the table if needed
		int addr_id = 0;

		// Get the country ID from the country table matching this address.

		// Is it safe to assume that the country that we are looking
		// for will be there?


		try {

			int addr_co_id = country;

			//Get address id for this customer, possible insert row in
			//address table
			PreparedStatement match_address = con.prepareStatement(benchmark_queries.get("@sql.enterAddress.match"));
			match_address.setString(1, street1);
			match_address.setString(2, street2);
			match_address.setString(3, city);
			match_address.setString(4, state);
			match_address.setString(5, zip);
			match_address.setInt(6, addr_co_id);
			ResultSet rs = match_address.executeQuery();
			if (!rs.next()) {//We didn't match an address in the addr table
				PreparedStatement insert_address_row = con.prepareStatement(benchmark_queries.get("@sql.enterAddress.insert"));
				insert_address_row.setString(2, street1);
				insert_address_row.setString(3, street2);
				insert_address_row.setString(4, city);
				insert_address_row.setString(5, state);
				insert_address_row.setString(6, zip);
				insert_address_row.setInt(7, addr_co_id);

				addr_id = id;
				//Need to insert a new row in the address table
				insert_address_row.setInt(1, addr_id);
				insert_address_row.executeUpdate();

				insert_address_row.close();
			} else { //We actually matched
				addr_id = rs.getInt("addr_id");
			}
			match_address.close();
			rs.close();
		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}
		return addr_id;
	}

	public void refreshSession(int C_ID) {
		try {


			PreparedStatement readStatment = getReadConnection().prepareStatement(benchmark_queries.get("@sql.getCustomer.login"));
			readStatment.setInt(1, C_ID);
			ResultSet set = readStatment.executeQuery();
			set.close();
			readStatment.close();

			// Prepare SQL
			Connection con = getWriteConnection();
			con.setAutoCommit(false);
			PreparedStatement updateLogin = con.prepareStatement(benchmark_queries.get("@sql.refreshSession"));


			// Set parameter
			updateLogin.setInt(1, C_ID);
			updateLogin.executeUpdate();

			con.commit();
			updateLogin.close();
			con.setAutoCommit(true);


		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}
	}


	public void BuyRequest(int shopping_id) {


		int sc_id = shopping_id;


		float cost = 0;
		int qty = 0;

		try {
			Connection con = getReadConnection();
			con.setAutoCommit(false);

			PreparedStatement readStatment = con.prepareStatement(benchmark_queries.get("@sql.getCartLines"));
			readStatment.setInt(1, sc_id);
			ResultSet set = readStatment.executeQuery();
			con.commit();
			while (set.next()) {

				int qty_read = set.getInt("SCL_QTY");
				qty += qty_read;
				cost += (qty_read * set.getFloat("SCL_COST"));


			}
			set.close();
			readStatment.close();


			float SC_SUB_TOTAL = cost * (1 - 0.2f);//cheats...
			float SC_TAX = SC_SUB_TOTAL * 0.0825f;
			float SC_SHIP_COST = 3.00f + (1.00f * qty);
			float SC_TOTAL = SC_SUB_TOTAL + SC_SHIP_COST + SC_TAX;


			PreparedStatement writeStatment = getWriteConnection().prepareStatement(benchmark_queries.get("@sql.updateCartInfo"));
			writeStatment.setFloat(1, SC_SUB_TOTAL);
			writeStatment.setFloat(2, SC_TAX);
			writeStatment.setFloat(3, SC_SHIP_COST);
			writeStatment.setFloat(4, SC_TOTAL);
			writeStatment.setInt(5, sc_id);

			writeStatment.execute();


		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}
	}

	public void BuyComfirm(int customer_id, int process_id, int cart_id) {

		try {
			Connection con = getWriteConnection();
			con.setAutoCommit(false);

			Map<String, Object> Cinfo = getCinfo(con, customer_id);
			Map<Integer, Map<String, Object>> cart = getCart(con, cart_id);

			int ship_addr_id = -1;
			float decision = random.nextFloat();
			if (decision < 0.2) {
				Address address = generateAddress();


				int id = getIDfromValues(node_id, process_id, addr_aux_id, Constants.NUM_ADDRESSES);
				addr_aux_id++;

				//String id =

				ship_addr_id = enterAddress(con, id,
						address.getStreet1(),
						address.getStreet2(),
						address.getCity(),
						address.getState(),
						address.getZip(),
						address.getCountry_id());
			} else {
				ship_addr_id = (Integer) Cinfo.get("C_ADDR_ID");
			}

			int c_addr = (Integer) Cinfo.get("C_ADDR_ID");
			double c_discount = (Double) Cinfo.get("C_DISCOUNT");

			String shipping = ship_types[random.nextInt(ship_types.length)];

			float SC_TOTAL = (Float) cart.get(-1).get("SC_TOTAL");

			int order_id = enterOrder(con, cart_id, customer_id, cart, SC_TOTAL, ship_addr_id, c_addr, shipping, c_discount);

			String cc_type = BenchmarkUtil.getRandomAString(10);
			long cc_number = BenchmarkUtil.getRandomNString(16);
			String cc_name = BenchmarkUtil.getRandomAString(30);
			Date cc_expiry = new Date(System.currentTimeMillis() + random.nextInt(644444400));


			enterCCXact(con, order_id, cc_type, cc_number, cc_name, cc_expiry, SC_TOTAL, ship_addr_id);
			con.commit();
			con.setAutoCommit(true);
		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}
	}


	private int enterOrder(Connection con, int order_id, int customer_id, Map<Integer, Map<String, Object>> cart, float SC_TOTAL, int ship_addr_id, int c_addr, String shipping, double c_discount) {


		float SC_SUB_TOTAL = (Float) cart.get(-1).get("SC_SUB_TOTAL");
		float SC_TAX = (Float) cart.get(-1).get("SC_TAX");


		try {
			PreparedStatement insert_row = con.prepareStatement(benchmark_queries.get("@sql.enterOrder.insert"));
			insert_row.setInt(2, customer_id);
			insert_row.setDouble(3, SC_SUB_TOTAL);
			insert_row.setDouble(4, SC_TOTAL);
			insert_row.setDouble(5, SC_TAX);

			insert_row.setString(6, shipping);
			insert_row.setInt(7, random.nextInt(7));
			insert_row.setInt(8, c_addr);
			insert_row.setInt(9, ship_addr_id);
			insert_row.setString(10, status_types[random.nextInt(status_types.length)]);


			insert_row.setInt(1, order_id);
			insert_row.executeUpdate();

			insert_row.close();
		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}

		int counter = 0;
		for (Integer item : cart.keySet()) {

			boolean done = false;
			int rounds = 0;
			try {
				Savepoint point = con.setSavepoint();


				while (!done) {

					if (item != -1) {

						int qty = (Integer) cart.get(item).get("SCL_QTY");

						// - Creates one or more 'order_line' rows.
						addOrderLine(con, counter, order_id, item,
								qty, c_discount,
								BenchmarkUtil.getRandomAString(20, 100));
						counter++;

						// - Adjusts the stock for each item ordered
						int stock = getStock(con, item);
						boolean sucess = false;

						if ((stock - qty) < 10) {
							sucess = setStock(con, item,
									stock - qty + 21);
						} else {
							sucess = setStock(con, item, stock - qty);
						}

						done = sucess;
						if (!sucess) {
							System.out.println("ERROR SET:" + cart.keySet().toString());
							con.rollback(point);
							rounds++;
						}
					} else {
						done = true;
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
			}
			if (rounds > 0) {
				System.out.println("Item " + item.intValue() + " stock update restarted " + rounds);
			}
		}
		return order_id;


	}

	public int getStock(Connection con, int i_id) {
		int stock = 0;
		try {
			PreparedStatement get_stock = con.prepareStatement(benchmark_queries.get("@sql.getStock"));

			// Set parameter
			get_stock.setInt(1, i_id);
			ResultSet rs = get_stock.executeQuery();

			// Results
			rs.next();
			stock = rs.getInt("i_stock");
			rs.close();
			get_stock.close();
		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}
		return stock;
	}

	public boolean setStock(Connection con, int i_id, int new_stock) throws SQLException {
		PreparedStatement update_row = null;
		try {
			update_row = con.prepareStatement(benchmark_queries.get("@sql.setStock"));
			update_row.setInt(1, new_stock);
			update_row.setInt(2, i_id);
			update_row.executeUpdate();
			update_row.close();
			return true;
		} catch (java.lang.Exception ex) {
			//           ex.printStackTrace();
			System.out.println("ERROR:" + ex.getMessage());
			update_row.close();
			return false;
		}
	}


	public void addOrderLine(Connection con,
							 int ol_id, int ol_o_id, int ol_i_id,
							 int ol_qty, double ol_discount, String ol_comment) {
		try {
			PreparedStatement insert_row = con.prepareStatement(benchmark_queries.get("@sql.addOrderLine"));

			insert_row.setInt(1, ol_id);
			insert_row.setInt(2, ol_o_id);
			insert_row.setInt(3, ol_i_id);
			insert_row.setInt(4, ol_qty);
			insert_row.setDouble(5, ol_discount);
			insert_row.setString(6, ol_comment);
			insert_row.executeUpdate();
			insert_row.close();
		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}
	}


	public void enterCCXact(Connection con,
							int o_id,		// Order id
							String cc_type,
							long cc_number,
							String cc_name,
							Date cc_expiry,
							double total,   // Total from shopping cart
							int ship_addr_id) {

		// Updates the CC_XACTS table
		if (cc_type.length() > 10)
			cc_type = cc_type.substring(0, 10);
		if (cc_name.length() > 30)
			cc_name = cc_name.substring(0, 30);

		try {
			// Prepare SQL
			PreparedStatement statement = con.prepareStatement(benchmark_queries.get("@sql.enterCCXact"));

			// Set parameter
			statement.setInt(1, o_id);		   // cx_o_id
			statement.setString(2, cc_type);	 // cx_type
			statement.setLong(3, cc_number);	 // cx_num
			statement.setString(4, cc_name);	 // cx_name
			statement.setDate(5, cc_expiry);	 // cx_expiry
			statement.setDouble(6, total);	   // cx_xact_amount
			statement.setInt(7, ship_addr_id);   // ship_addr_id
			statement.executeUpdate();
			statement.close();
		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}

	}


	private Map<Integer, Map<String, Object>> getCart(Connection con, int SHOPPING_ID) {

		TreeMap<Integer, Map<String, Object>> cart = new TreeMap<Integer, Map<String, Object>>();


		try {
			PreparedStatement get_cart = con.prepareStatement(benchmark_queries.get("@sql.getCart"));
			get_cart.setInt(1, SHOPPING_ID);
			ResultSet rs = get_cart.executeQuery();


			TreeMap<String, Object> values = new TreeMap<String, Object>();
			rs.next();
			values.put("SC_SUB_TOTAL", rs.getFloat("SC_SUB_TOTAL"));
			values.put("SC_TAX", rs.getFloat("SC_TAX"));
			values.put("SC_SHIP_COST", rs.getFloat("SC_SHIP_COST"));
			values.put("SC_TOTAL", rs.getFloat("SC_TOTAL"));
			cart.put(-1, values);

			rs.close();
			get_cart.close();

			//cart lines

			PreparedStatement get_cart_lines = con.prepareStatement(benchmark_queries.get("@sql.getCartLines"));
			get_cart_lines.setInt(1, SHOPPING_ID);
			rs = get_cart_lines.executeQuery();

			while (rs.next()) {
				TreeMap<String, Object> cl_values = new TreeMap<String, Object>();
				int item = rs.getInt("SCL_I_ID");
				cl_values.put("SCL_COST", rs.getFloat("SCL_COST"));
				cl_values.put("SCL_QTY", rs.getInt("SCL_QTY"));
				cart.put(item, cl_values);
			}

			rs.close();
			get_cart_lines.close();
		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}

		return cart;
	}


	public Map<String, Object> getCinfo(Connection con, int c_id) {
		TreeMap<String, Object> info = new TreeMap<String, Object>();

		double c_discount = 0.0;
		try {
			// Prepare SQL
			PreparedStatement statement = con.prepareStatement(benchmark_queries.get("@sql.getCInfo"));

			// Set parameter
			statement.setInt(1, c_id);
			ResultSet rs = statement.executeQuery();

			// Results
			rs.next();
			info.put("C_FNAME", rs.getString("C_FNAME"));
			info.put("C_LNAME", rs.getString("C_LNAME"));
			info.put("C_DISCOUNT", rs.getDouble("C_DISCOUNT"));
			info.put("C_ADDR_ID", rs.getInt("C_ADDR_ID"));


			rs.close();
			statement.close();

		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}
		return info;
	}

	public void OrderInquiry(int customer) {	   //1h


		try {
			int order_id;

			// Prepare SQL
			Connection con = getReadConnection();
			con.setAutoCommit(false);


			//	    System.out.println("cust_id: " + getCustomer(c_uname).c_id);

			{
				// *** Get the o_id of the most recent order for this user
				PreparedStatement get_most_recent_order_id = con.prepareStatement(benchmark_queries.get("@sql.getMostRecentOrder.id"));

				// Set parameter
				get_most_recent_order_id.setInt(1, customer);
				ResultSet rs = get_most_recent_order_id.executeQuery();

				if (rs.next()) {
					order_id = rs.getInt("o_id");
					//     System.out.println("ORDER_ID:" + order_id);
				} else {
					// There is no most recent order
					rs.close();
					get_most_recent_order_id.close();
					con.commit();
					// System.out.println("ERROR ON ORDER INQUIRY");
					return;
				}
				rs.close();
				get_most_recent_order_id.close();
			}

			{
				// *** Get the order info for this o_id
				PreparedStatement get_order = con.prepareStatement(benchmark_queries.get("@sql.getMostRecentOrder.order"));


				// Set parameter
				get_order.setInt(1, order_id);
				ResultSet rs2 = get_order.executeQuery();

				// Results
				if (!rs2.next()) {
					// FIXME - This case is due to an error due to a database population error
					con.commit();
					rs2.close();
					System.out.println("ERROR2 ON ORDER INQUIRY");
					return;
				}
				rs2.close();
				get_order.close();
			}

			{
				// *** Get the order_lines for this o_id
				PreparedStatement get_order_lines = con.prepareStatement(benchmark_queries.get("@sql.getMostRecentOrder.lines"));

				// Set parameter
				get_order_lines.setInt(1, order_id);
				ResultSet rs3 = get_order_lines.executeQuery();

				// Results
				while (rs3.next()) {
					//   System.out.println("RS3:"+rs3.getMetaData().getColumnName(1));
				}
				rs3.close();
				get_order_lines.close();
			}

			con.commit();
			con.setAutoCommit(true);
		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}

	}


	public void doSearch(String term, String field) {   //30


		Connection connection = getReadConnection();

		if (term.equalsIgnoreCase("SUBJECT")) {
			try {
				connection.setAutoCommit(false);

				PreparedStatement statement = connection.prepareStatement(benchmark_queries.get("@sql.doSubjectSearch"));

				// Set parameter
				statement.setString(1, field);
				ResultSet rs = statement.executeQuery();

				// Results
				while (rs.next()) {
				}
				rs.close();
				connection.commit();
				statement.close();
			} catch (java.lang.Exception ex) {
				ex.printStackTrace();
			}

		} else if (term.equalsIgnoreCase("AUTHOR")) {
			try {

				connection.setAutoCommit(false);
				PreparedStatement statement = connection.prepareStatement(benchmark_queries.get("@sql.doAuthorSearch"));

				// Set parameter
				statement.setString(1, field);
				ResultSet rs = statement.executeQuery();

				// Results
				while (rs.next()) {
				}
				rs.close();
				connection.commit();
				statement.close();
			} catch (java.lang.Exception ex) {
				ex.printStackTrace();
			}

		} else if (term.equalsIgnoreCase("TITLE")) {
			try {

				connection.setAutoCommit(false);
				PreparedStatement statement = connection.prepareStatement(benchmark_queries.get("@sql.doTitleSearch"));

				// Set parameter
				statement.setString(1, field);
				ResultSet rs = statement.executeQuery();

				// Results
				while (rs.next()) {
				}
				rs.close();
				connection.commit();
				statement.close();
			} catch (java.lang.Exception ex) {
				ex.printStackTrace();
			}
		} else {
			System.out.println("OPTION NOT RECOGNIZED");

		}
	}

	private void newProducts(String field) {
		try {
			Connection connection = getReadConnection();
			connection.setAutoCommit(false);

			PreparedStatement statement = connection.prepareStatement(benchmark_queries.get("@sql.getNewProducts"));

			statement.setString(1, field);
			ResultSet rs = statement.executeQuery();

			while (rs.next()) {
//                System.out.println("I:"+rs.getInt("i_id"));

			}
			rs.close();
			connection.commit();
			statement.close();
		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}
	}

	public void BestSellers(String field) {
		try {
			Connection connection = getReadConnection();
			connection.setAutoCommit(false);

			PreparedStatement statement = connection.prepareStatement(benchmark_queries.get("@sql.getBestSellers"));

			statement.setInt(1, 3333);
			statement.setString(2, field);
			ResultSet rs = statement.executeQuery();

			while (rs.next()) {
				//  System.out.println("I:"+rs.getInt("i_id"));
			}
			rs.close();
			connection.commit();
			statement.close();

		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}
	}

	public void ItemInfo(int id) {
		try {
			Connection connection = getReadConnection();
			connection.setAutoCommit(false);
			PreparedStatement statement = connection.prepareStatement(benchmark_queries.get("@sql.getBook"));

			statement.setInt(1, id);
			ResultSet rs = statement.executeQuery();

			// Results
			while (rs.next()) {
			}
			rs.close();
			connection.commit();
			statement.close();

		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}

	}

	public void AdminChange(int item_id) {
		String original_name = Thread.currentThread().getName();
		Thread.currentThread().setName(original_name + "admin");

		try {
			Connection connection = getWriteConnection();
			connection.setAutoCommit(false);


			//get item info

			PreparedStatement read_statement = connection.prepareStatement(benchmark_queries.get("@sql.getBook"));

			read_statement.setInt(1, item_id);
			ResultSet read_rs = read_statement.executeQuery();

			while (read_rs.next()) {
			}
			read_rs.close();
			connection.commit();
			read_statement.close();


			//update


			PreparedStatement related = connection.prepareStatement(benchmark_queries.get("@sql.adminUpdate.related"));


			// Set parameter
			related.setInt(1, item_id);
			related.setInt(2, item_id);
			ResultSet rs = related.executeQuery();

			int[] related_items = new int[5];
			// Results
			int counter = 0;
			int last = 0;
			while (rs.next()) {
				last = rs.getInt(1);
				related_items[counter] = last;
				counter++;
			}

			// This is the case for the situation where there are not 5 related books.
			for (int i = counter; i < 5; i++) {
				last++;
				related_items[i] = last;
			}
			rs.close();

			related.close();

			PreparedStatement update_statement1 = connection.prepareStatement(benchmark_queries.get("@sql.adminUpdate"));


			float I_COST = random.nextInt(100);
			String image = new String("img" + random.nextInt(1000) % 100 + "/image_" + random.nextInt(1000) + ".gif");
			String thumb = image.replace("image", "thumb");

			PreparedStatement update_statement2 = connection.prepareStatement(benchmark_queries.get("@sql.adminUpdate.related1"));

			// Set parameter
			update_statement1.setDouble(1, I_COST);
			update_statement1.setString(2, image);
			update_statement1.setString(3, thumb);
			update_statement1.setInt(4, item_id);
			update_statement1.executeUpdate();
			update_statement1.close();


			update_statement2.setInt(1, related_items[0]);
			update_statement2.setInt(2, related_items[1]);
			update_statement2.setInt(3, related_items[2]);
			update_statement2.setInt(4, related_items[3]);
			update_statement2.setInt(5, related_items[4]);
			update_statement2.setInt(6, item_id);
			update_statement2.executeUpdate();

			update_statement2.close();
			connection.commit();
			connection.setAutoCommit(true);
			Thread.currentThread().setName(original_name);


		} catch (java.lang.Exception ex) {
			ex.printStackTrace();
		}


	}


	private void addIndexes() {

		Connection con = getWriteConnection();
		boolean error = false;

		try {
			con.setAutoCommit(false);
		} catch (SQLException e) {
		}

		try {
			PreparedStatement statement1 = con.prepareStatement
					("create index author_a_lname on author(a_lname)");
			statement1.executeUpdate();
			PreparedStatement statement2 = con.prepareStatement
					("create index address_addr_co_id on address(addr_co_id)");
			statement2.executeUpdate();
			PreparedStatement statement3 = con.prepareStatement
					("create index addr_zip on address(addr_zip)");
			statement3.executeUpdate();
			PreparedStatement statement4 = con.prepareStatement
					("create index customer_c_addr_id on customer(c_addr_id)");
			statement4.executeUpdate();
			PreparedStatement statement5 = con.prepareStatement
					("create index customer_c_uname on customer(c_uname)");
			statement5.executeUpdate();
			PreparedStatement statement6 = con.prepareStatement
					("create index item_i_title on item(i_title)");
			statement6.executeUpdate();
			PreparedStatement statement7 = con.prepareStatement
					("create index item_i_subject on item(i_subject)");
			statement7.executeUpdate();
			PreparedStatement statement8 = con.prepareStatement
					("create index item_i_a_id on item(i_a_id)");
			statement8.executeUpdate();
			PreparedStatement statement9 = con.prepareStatement
					("create index order_line_ol_i_id on order_line(ol_i_id)");
			statement9.executeUpdate();
			PreparedStatement statement10 = con.prepareStatement
					("create index order_line_ol_o_id on order_line(ol_o_id)");
			statement10.executeUpdate();
			PreparedStatement statement11 = con.prepareStatement
					("create index country_co_name on country(co_name)");
			statement11.executeUpdate();
			PreparedStatement statement12 = con.prepareStatement
					("create index orders_o_c_id on orders(o_c_id)");
			statement12.executeUpdate();
			PreparedStatement statement13 = con.prepareStatement
					("create index scl_i_id on shopping_cart_line(scl_i_id)");
			statement13.executeUpdate();

			con.commit();
		} catch (java.lang.Exception ex) {
			error = true;
		}
		if (!error) {
			System.out.println("Indexes created");
		}

		try {
			con.setAutoCommit(true);
		} catch (SQLException e) {
		}
	}


	public void remove_and_create_Database() {
		Connection connection = getRawConnection();

		boolean error = false;

		try {
			PreparedStatement statement = connection.prepareStatement
					("DROP DATABASE tpcw");

			statement.executeUpdate();
		} catch (java.lang.Exception ex) {
			error = true;
		}
		if (!error) {
			System.out.println("Deleted database tpcw");
		}
		error = false;

		try {
			PreparedStatement statement = connection.prepareStatement
					("CREATE DATABASE tpcw");

			statement.executeUpdate();
		} catch (java.lang.Exception ex) {
			error = true;
		}
		if (!error) {
			System.out.println("Created database tpcw");
		}
		error = false;
	}

	public void createDatabase() {

		Connection connection = getRawConnection();

		boolean error = false;

		try {
			PreparedStatement statement = connection.prepareStatement
					("CREATE SCHEMA tpcw");

			statement.executeUpdate();
		} catch (java.lang.Exception ex) {
			error = true;
		}
		if (!error) {
			System.out.println("Created Schema tpcw");
		}
		error = false;
	}


	public void createTables() {
		int i;

		Connection con = getWriteConnection();
		try {
			con.setAutoCommit(false);
		} catch (SQLException e) {
		}


		boolean error = false;

		try {
			PreparedStatement statement = con.prepareStatement
					("CREATE TABLE ADDRESS ( addr_id int not null, addr_street1 varchar(40), addr_street2 varchar(40), addr_city varchar(30), addr_state varchar(20), addr_zip varchar(10), addr_co_id int, primary key(addr_id)) engine=innodb");
			// ("CREATE TABLE ADDRESS ( ADDR_ID VARCHAR(10) not null, ADDR_STREET1 varchar(40), ADDR_STREET2 varchar(40), ADDR_CITY varchar(30), ADDR_STATE varchar(20), ADDR_ZIP varchar(10), ADDR_CO_ID int, PRIMARY KEY(ADDR_ID))");

			statement.executeUpdate();
			con.commit();
		} catch (java.lang.Exception ex) {
			error = true;

		}
		if (!error) {
			System.out.println("Created table ADDRESS");
		}
		error = false;


		try {
			PreparedStatement statement = con.prepareStatement
					("CREATE TABLE AUTHOR ( a_id int not null, a_fname varchar(20), a_lname varchar(20), a_mname varchar(20), a_dob date, a_bio varchar(500), primary key(a_id)) ENGINE=innodb");


			statement.executeUpdate();
			con.commit();
			System.out.println("Created table AUTHOR");
		} catch (java.lang.Exception ex) {
			error = true;
		}
		if (!error) {
			System.out.println("Created table AUTHOR");

		}
		error = false;


		try {
			PreparedStatement statement = con.prepareStatement
					("CREATE TABLE CC_XACTS ( cx_o_id int not null, cx_type varchar(10), cx_num varchar(20), cx_name varchar(30), cx_expiry date, cx_auth_id char(15), cx_xact_amt double, cx_xact_date date, cx_co_id int, primary key(cx_o_id)) ENGINE=innodb");

			statement.executeUpdate();
			con.commit();

		} catch (java.lang.Exception ex) {
			error = true;
		}
		if (!error) {
			System.out.println("Created table CC_XACTS");
		}
		error = false;

		try {
			PreparedStatement statement = con.prepareStatement
					("CREATE TABLE COUNTRY ( co_id int not null, co_name varchar(50), co_exchange double, co_currency varchar(18), primary key(co_id)) ENGINE=innodb");

			statement.executeUpdate();
			con.commit();
		} catch (java.lang.Exception ex) {
			error = true;
		}
		if (!error) {
			System.out.println("Created table COUNTRY");
		}
		error = false;

		try {
			PreparedStatement statement = con.prepareStatement
					("CREATE TABLE CUSTOMER ( c_id int not null, c_uname varchar(20), c_passwd varchar(20), c_fname varchar(17), c_lname varchar(17), c_addr_id int, c_phone varchar(18), c_email varchar(50), c_since date, c_last_visit date, c_login timestamp, c_expiration timestamp, c_discount real, c_balance double, c_ytd_pmt double, c_birthdate date, c_data varchar(510), primary key(c_id)) ENGINE=innodb");
//            ("CREATE TABLE CUSTOMER ( C_ID VARCHAR(10) not null, C_UNAME varchar(20), C_PASSWD varchar(20), C_FNAME varchar(17), C_LNAME varchar(17), C_ADDR_ID VARCHAR(10), C_PHONE varchar(18), C_EMAIL varchar(50), C_SINCE date, C_LAST_VISIT date, C_LOGIN timestamp, C_EXPIRATION timestamp, C_DISCOUNT real, C_BALANCE double, C_YTD_PMT double, C_BIRTHDATE date, C_DATA varchar(510), PRIMARY KEY(C_ID))");
			statement.executeUpdate();
			con.commit();
		} catch (java.lang.Exception ex) {
			error = true;
		}
		if (!error) {
			System.out.println("Created table CUSTOMER");
		}
		error = false;

		try {
			PreparedStatement statement = con.prepareStatement
					("CREATE TABLE ITEM ( i_id int not null, i_title varchar(60), i_a_id int, i_pub_date date, i_publisher varchar(60), i_subject varchar(60), i_desc varchar(500), i_related1 int, i_related2 int, i_related3 int, i_related4 int, i_related5 int, i_thumbnail varchar(40), i_image varchar(40), i_srp double, i_cost double, i_avail date, i_stock int, i_isbn char(13), i_page int, i_backing varchar(15), i_dimension varchar(25), primary key(i_id)) ENGINE=innodb");
			statement.executeUpdate();
			con.commit();
		} catch (java.lang.Exception ex) {
			error = true;
		}
		if (!error) {
			System.out.println("Created table ITEM");
		}
		error = false;

		try {
			PreparedStatement statement = con.prepareStatement
					("CREATE TABLE ORDER_LINE ( ol_id int not null, ol_o_id int not null, ol_i_id int, ol_qty int, ol_discount double, ol_comments varchar(110), primary key(ol_id, ol_o_id)) ENGINE=innodb");
			statement.executeUpdate();
			con.commit();
		} catch (java.lang.Exception ex) {
			error = true;
		}
		if (!error) {
			System.out.println("Created table ORDER_LINE");
		}
		error = false;


		try {
			PreparedStatement statement = con.prepareStatement
					("CREATE TABLE ORDERS ( o_id int not null, o_c_id int, o_date date, o_sub_total double, o_tax double, o_total double, o_ship_type varchar(10), o_ship_date date, o_bill_addr_id int, o_ship_addr_id int, o_status varchar(15), primary key(o_id)) ENGINE=innodb");
			//         ("CREATE TABLE ORDERS ( O_ID varchar(10) not null, O_C_ID varchar(10), O_DATE date, O_SUB_TOTAL double, O_TAX double, O_TOTAL double, O_SHIP_TYPE varchar(10), O_SHIP_DATE date, O_BILL_ADDR_ID varchar(10), O_SHIP_ADDR_ID varchar(10), O_STATUS varchar(15), PRIMARY KEY(O_ID))");
			statement.executeUpdate();
			con.commit();
		} catch (java.lang.Exception ex) {
			error = true;
		}
		if (!error) {
			System.out.println("Created table ORDERS");
		}
		error = false;

		try {
			PreparedStatement statement = con.prepareStatement
					("CREATE TABLE SHOPPING_CART ( sc_id int not null, sc_time timestamp, sc_sub_total float default null, sc_tax float default 0 , sc_ship_cost float default 0 , sc_total float default 0 , primary key(sc_id)) ENGINE=innodb");
			statement.executeUpdate();
			con.commit();
		} catch (java.lang.Exception ex) {
			error = true;
		}
		if (!error) {
			System.out.println("Created table SHOPPING_CART");
		}
		error = false;


		try {
			PreparedStatement statement = con.prepareStatement
					("CREATE TABLE SHOPPING_CART_LINE ( scl_sc_id int not null, scl_qty int, scl_i_id int not null,scl_cost float default 0 , scl_srp double default 0 ,  scl_title mediumtext , scl_backing mediumtext , primary key(scl_sc_id, scl_i_id)) ENGINE=innodb");
			statement.executeUpdate();
			con.commit();
		} catch (java.lang.Exception ex) {
			error = true;
		}
		if (!error) {
			System.out.println("Created table SHOPPING_CART_LINE");
		}
		error = false;


		try {
			PreparedStatement statement = con.prepareStatement
					("CREATE TABLE RESULTS ( client_id int not null, item_id int not null, stock int not null, bought int not null, primary key( item_id,client_id)) engine=innodb");
			statement.executeUpdate();
			con.commit();
			System.out.println("Created table RESULTS");
		} catch (java.lang.Exception ex) {
			error = true;
		}
		if (!error) {
			System.out.println("Created table SHOPPING_CART_LINE");
		}
		error = false;

		try {
			con.setAutoCommit(true);
		} catch (SQLException e) {
		}


	}


}
