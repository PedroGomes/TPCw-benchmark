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


/*
 * *********************************************************************
 * Copyright (c) 2011 Valter Balegas and Universidade Nova de Lisboa.
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

package pt.fct.di.benchmarks.TPCW_Riak.database;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
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

import pt.fct.di.benchmarks.TPCW_Riak.entities.Address;
import pt.fct.di.benchmarks.TPCW_Riak.entities.Author;
import pt.fct.di.benchmarks.TPCW_Riak.entities.AuthorIndex;
import pt.fct.di.benchmarks.TPCW_Riak.entities.BestSellerEntry;
import pt.fct.di.benchmarks.TPCW_Riak.entities.BestSellerSubject;
import pt.fct.di.benchmarks.TPCW_Riak.entities.CCXact;
import pt.fct.di.benchmarks.TPCW_Riak.entities.CCXactItem;
import pt.fct.di.benchmarks.TPCW_Riak.entities.Customer;
import pt.fct.di.benchmarks.TPCW_Riak.entities.Item;
import pt.fct.di.benchmarks.TPCW_Riak.entities.Order;
import pt.fct.di.benchmarks.TPCW_Riak.entities.OrderLine;
import pt.fct.di.benchmarks.TPCW_Riak.entities.SCLine;
import pt.fct.di.benchmarks.TPCW_Riak.entities.ShoppingCart;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.bucket.FetchBucket;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.http.RiakConfig;
import com.basho.riak.client.mapreduce.JavascriptFunction;
import com.basho.riak.client.mapreduce.filter.StartsWithFilter;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.pbc.PBClientAdapter;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.basho.riak.client.response.MapReduceResponse;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

//Falta o consistency level em todos os fetchs!
public class TPCW_Riak_Executor implements DatabaseExecutorInterface {

	private static final int BESTSELLERMAX = 50;
	private String keyspace;
	private Map<String, Integer> connections;
	private KeyGenerator keyGenerator;
	private ArrayList<IRiakClient> iclients;
	private ArrayList<RawClient> clients;
	//private ArrayList<RiakClient> swiftClients;
	private Map<String, String> paths;
	private TPM_counter counter;
	private long simulatedDelay;
	private int search_slice_ratio;
	private int global_executor_counter;
	private int executor_id;
	private ResultHandler client_result_handler;
	private Gson GSON = new Gson();

	//TODO: Não está implementado
	public static int INSERT_CONSISTENCY_LEVEL = 2;
	public static int REMOVE_CONSISTENCY_LEVEL = 2;
	public static int RANGE_CONSISTENCY_LEVEL = 2;
	public static int TRANSACTIONAL_CONSISTENCY_LEVEL = 2;
	public static int READ_CONSISTENCY_LEVEL = 2;
	public static int WRITE_CONSISTENCY_LEVEL = 2;
	public static int ALL = 10;

	private PBClusterConfig clusterConf;

	Map<String, Integer> partialBought = new TreeMap<String, Integer>();
	ArrayList<String> operations = new ArrayList<String>();
	int bought_qty;
	int bought_actions;
	int bought_carts;
	int zeros;
	private int debug_bougth_items;
	private int total_bought;
	int last, lastRaw = 0;

	private String[] credit_cards = { "VISA", "MASTERCARD", "DISCOVER", "AMEX",
			"DINERS" };
	private String[] ship_types = { "AIR", "UPS", "FEDEX", "SHIP", "COURIER",
			"MAIL" };
	private String[] status_types = { "PROCESSING", "SHIPPED", "PENDING",
			"DENIED" };

	private Random random = new Random();

	public TPCW_Riak_Executor(String keyspace,
			Map<String, Integer> connections,
		//	ConsistencyLevel[] consistencyLevels,
			Map<String, String> key_paths, int think_time, int search_slices,
			KeyGenerator keyGenerator, TPM_counter tpm_counter) {

		this.keyspace = keyspace;
		this.keyGenerator = keyGenerator;
		this.connections = connections;
//		INSERT_CONSISTENCY_LEVEL = consistencyLevels[0];
//		REMOVE_CONSISTENCY_LEVEL = consistencyLevels[1];
//		RANGE_CONSISTENCY_LEVEL = consistencyLevels[2];
//		TRANSACTIONAL_CONSISTENCY_LEVEL = consistencyLevels[3];
//		READ_CONSISTENCY_LEVEL = consistencyLevels[4];
//		WRITE_CONSISTENCY_LEVEL = consistencyLevels[5];
		this.iclients = new ArrayList<IRiakClient>();
		this.clients = new ArrayList<RawClient>();
		this.connections = connections;
		this.keyspace = keyspace;
		this.paths = key_paths;
		this.counter = tpm_counter;

		Map<String, Integer> sortedConnections = BenchmarkUtil
				.randomizeMap(connections);

		for (int i = 0; i < 10; i++) {
			for (String rand_host : sortedConnections.keySet()) {
				String host = rand_host.split(":")[1];
				try {
					IRiakClient client = RiakFactory.pbcClient();
					iclients.add(client);
				} catch (RiakException e) {
					e.printStackTrace();
				}

			}
		}

		for (int i = 0; i < 10; i++) {
			for (String rand_host : sortedConnections.keySet()) {
				com.basho.riak.pbc.RiakClient pbcClient;
				try {
					pbcClient = new com.basho.riak.pbc.RiakClient(rand_host.split(":")[1]);
					RawClient rawClient = null;
					rawClient = new PBClientAdapter(pbcClient);
					clients.add(rawClient);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

			}
		}

		search_slice_ratio = search_slices;
		simulatedDelay = think_time;

	}

	@Override
	public void start(WorkloadGeneratorInterface workload,
			BenchmarkNodeID nodeId, int operation_number, ResultHandler handler) {

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
				client_result_handler.logResult(op.getOperation(),
						(end_time - init_time));

				simulatedDelay = ThinkTime.getThinkTime();

				if (simulatedDelay > 0) {
					Thread.sleep(simulatedDelay);
				}

			} catch (NoSuchFieldException e) {
				System.out.println("[ERROR:] THIS OPERATION DOES NOT EXIST: "
						+ e.getMessage());
				break;
			} catch (InterruptedException e) {
				System.out
						.println("[ERROR:] THINK TIME AFTER METHOD EXECUTION INTERRUPTED: "
								+ e.getMessage());
				break;

			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("-- Error : Client " + executor_id
						+ " going down....");
				break;

			}
			long end_time = System.currentTimeMillis();
			counter.increment();
			client_result_handler.logResult("OPERATIONS",
					(end_time - g_init_time));

		}

		client_result_handler.getResulSet().put("bought", partialBought);
		client_result_handler.getResulSet().put("total_bought", bought_qty);
		client_result_handler.getResulSet().put("buying_actions",
				bought_actions);
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
			Map<String, Map<String, Object>> items_info = rangeQuery("Item",
					fields, -1);

			op.setResult(items_info);

		} else if (method_name
				.equalsIgnoreCase("Get_Stock_And_Products_after_increment")) {

			ArrayList<String> fields = new ArrayList<String>();
			int stock = (Integer) op.getParameter("STOCK");
			setItemStocks(stock);

			System.out.println("sleeeping after stock reposition...");
			Thread.sleep(60000);

			fields.add("I_STOCK");
			Map<String, Map<String, Object>> items_info = rangeQuery("item",
					fields, -1);
			op.setResult(items_info);

		} else if (method_name.equalsIgnoreCase("GET_ITEM_STOCK")) {

			String item_id = (String) op.getParameters().get("ITEM_ID");

			Object o = read(item_id, "item", "I_STOCK", null);
			int stock = -1;
			if (o != null) {
				stock = (Integer) o;
			}
			op.setResult(stock);

		} else if (method_name.equalsIgnoreCase("ADD_TO_CART")) {

			String cart = (String) op.getParameter("CART_ID");
			String item_id = (String) op.getParameter("ITEM_ID");
			int qty = (Integer) op.getParameter("QTY");

			addToCart(cart, item_id, qty);

		} else if (method_name.equalsIgnoreCase("BUY_CART")) {

			bought_carts++;
			String cart_id = (String) op.getParameter("CART_ID");

			Customer c = null;

			if (op.getParameters().containsKey("Customer")) {
				c = (Customer) op.getParameter("Customer");
			}
			buyCart(cart_id, c);

		} else if (method_name.equalsIgnoreCase("GET_BENCHMARK_RESULTS")) {
			// op.setResult(getResults());

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

			// TODO: Para descomentar refreshSession(customer);

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

			// TODO: Para descomentar doSearch(term, field);

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
			System.out.println("[WARN:]UNKNOWN REQUESTED METHOD: "
					+ method_name);

		}
	}

	@Override
	public Object insert(String key, String path, Entity value)
			throws Exception {

		TreeMap<String, Object> values_to_insert = value.getValuesToInsert();
		path = path.toLowerCase();
		modifyOrInsertListOfColumns(key, path, values_to_insert,
				INSERT_CONSISTENCY_LEVEL);

		return null;
	}

	@Override
	public void remove(String key, String path, String column) throws Exception {
		List<String> columns = null;
		if (column != null) {
			columns = new ArrayList<String>();
			columns.add(column);
		}
		removeKeyOrColumn(key, path, columns, REMOVE_CONSISTENCY_LEVEL);
	}

	public void update(String key, String bucketName, String column,
			Object value, String superfield) throws Exception {

		if (superfield != null) {
			System.out.println("Não é suposto acontecer");
		}
		Bucket bucket = getBucket(bucketName);
		String objectString = bucket.fetch(key).execute().getValueAsString();
		Entity entity = (Entity) GSON.fromJson(objectString,
				getEntityClass(bucketName));
		insertOrModifyAttribute(entity, value, column, WRITE_CONSISTENCY_LEVEL);
		getBucket(bucketName).store(key, GSON.toJson(entity).toString())
				.execute();
	}

	public Object read(String key, String bucketName, String column,
			String superfield) throws Exception {
		Bucket bucket = getBucket(bucketName);
		Entity entity = (Entity) GSON.fromJson(bucket.fetch(key).execute()
				.getValueAsString(), getEntityClass(bucketName));
		Field fld = getEntityClass(bucketName).getField(column);
		Object ret = fld.get(entity);
		return ret;

	}

	@Override
	public Map<String, Map<String, Object>> rangeQuery(String bucketName,
			List<String> fields, int limit) throws Exception {
		int i = 0;
		Iterable<String> it = getRawClient().listKeys(bucketName);
		Bucket bucket = getBucket(bucketName);
		Map<String, Map<String, Object>> results = new HashMap<String, Map<String, Object>>();
		for (String key : it) {
			IRiakObject objRiak = bucket.fetch(key).execute();
			Entity obj = (Entity) GSON.fromJson(objRiak.getValueAsString(),
					getEntityClass(bucketName));

			Map<String, Object> columnValues = new HashMap<String, Object>();
			for (Entry<String, Object> pair : obj.getValuesToInsert()
					.entrySet()) {
				columnValues.put(pair.getKey(), pair.getValue());
			}
			results.put(key, columnValues);
			i++;
			if (!(i < limit || limit == -1)) {
				break;
			}
		}
		return results;
	}

	@Override
	public void truncate(String bucketName) throws Exception {
		Bucket bucket = getBucket(bucketName);
		Iterable<String> it = bucket.keys();
		for (String key : it) {
			removeKeyOrColumn(key, bucketName, null, WRITE_CONSISTENCY_LEVEL);
		}

	}

	@Override
	public void index(String key, String path, Object value) throws Exception {

	}

	@Override
	public void index(String key, String path, String indexed_key,
			Map<String, Object> value) throws Exception {
		// TODO Auto-generated method stub

	}

	public void index(String key, String path, String indexed_key,
			AuthorIndex entity) throws Exception {

	}

	@Override
	public void closeClient() {
	}

	@Override
	public Map<String, String> getInfo() {
		TreeMap<String, String> info = new TreeMap<String, String>();
		return info;
	}

	/********************************************************/
	/**** TPCW benchmark consistency and old operations ****/
	/**
	 * ****************************************************
	 */

	public void setItemStocks(int initial_stock) throws Exception {

		ArrayList<String> fields = new ArrayList<String>();
		fields.add("I_STOCK");
		Map<String, Map<String, Object>> items_info = rangeQuery("item",
				fields, -1);

		for (String key : items_info.keySet()) {
			insert(initial_stock, key, "item", "I_STOCK", ALL);
		}

	}

	/*
	 * Adiciona uma certa quantidade de itens a um carrinho
	 */
	public void addToCart(String cart, String item, int qty_to_add)
			throws Exception {
		int itemInt = Integer.parseInt(item);

		Bucket bucket = getBucket("shopping_cart");
		IRiakObject obj = bucket.fetch(cart).execute();
		ShoppingCart sc = GSON.fromJson(obj.getValueAsString(),
				ShoppingCart.class);

		SCLine sc_line = sc.getSCLine(itemInt);
		sc_line.setSCL_QTY(sc_line.getSCL_QTY() + qty_to_add);
		getBucket("shopping_cart").store(cart, GSON.toJson(sc).toString())
				.execute();
	}

	/*
	 * Actualiza Stock e quantidade de itens vendidos
	 */
	public BuyingResult BuyCartItem(String item, int qty) throws Exception {
		total_bought++;

		try {
			Item itemObj = (Item) GSON.fromJson(getBucket("item").fetch(item)
					.execute().getValueAsString(), Item.class);

			if (itemObj == null) {
				return BuyingResult.DOES_NOT_EXIST;
			}
			int stock = itemObj.getI_STOCK();
			if ((stock - qty) >= 0) { // if stock is sufficient

				if (stock - qty == 0) {
					zeros++;
				}

				stock -= qty;
				itemObj.setI_STOCK(stock);
				itemObj.setI_TOTAL_SOLD(itemObj.getI_TOTAL_SOLD() + qty);

				BestSellerEntry bse = new BestSellerEntry(
						itemObj.getI_SUBJECT(), Integer.parseInt(item),
						itemObj.getI_TOTAL_SOLD());

				getBucket("BestSellersBuff").store(item + "",
						GSON.toJson(bse).toString()).execute();

				getBucket("item").store(item, GSON.toJson(itemObj).toString());

			} else {
				return BuyingResult.NOT_AVAILABLE;
			}
			debug_bougth_items++;
			return BuyingResult.BOUGHT;

		} catch (Exception e) {
			return BuyingResult.CANT_COMFIRM;
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
		client_result_handler.record_unstructured_data("BOUGHT_ITEMS_TIMELINE",
				item_id + "", buying_information);

		return result;
	}

	// Compra os itens do carrinho
	// actualiza o stock, e os tops
	public void buyCart(String cart_id, Customer c) throws Exception {

		Bucket bucket = getBucket("shopping_cart");
		ShoppingCart shop_c = (ShoppingCart) GSON.fromJson(
				bucket.fetch(cart_id + "").execute().getValueAsString(),
				ShoppingCart.class);
		/*
		 * Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		 * Set<String> list = new HashSet<String>(); list.add(shop_c.SC_C_ID);
		 * map.put("shopping_cart", list); MapReduceResponse res =
		 * getRiakClient().mapReduceOverObjects(map) .link("sc_lines",
		 * "cartline", false)
		 * .map(JavascriptFunction.named("Riak.mapValuesJson"), true) .submit();
		 */
		for (SCLine line : shop_c.SCLine) {
			BuyingResult result = buyItem(line.I_ID + "", line.SCL_QTY);
			client_result_handler.countEvent("BUYING_COUNTERS", result.name(),
					1);

			if (result.equals(BuyingResult.BOUGHT)) {
				bought_qty += line.SCL_QTY;
				bought_actions++;
				if (!partialBought.containsKey(line.I_ID)) {
					partialBought.put(line.I_ID + "", line.SCL_QTY);

				} else {
					int bought = partialBought.get(line.I_ID + "");
					partialBought.put(line.I_ID + "", (line.SCL_QTY + bought));
				}

			}
		}
	}

	/*
	 * 
	 * public Map<String, Map<String, Map<String, Object>>> getResults() throws
	 * Exception {
	 * 
	 * String column_family = "results"; Map<String, Map<String, Map<String,
	 * Object>>> results = new TreeMap<String, Map<String, Map<String,
	 * Object>>>(); SlicePredicate predicate = new SlicePredicate(); SliceRange
	 * slice_range = new SliceRange("".getBytes(), "".getBytes(), false, 10500);
	 * 
	 * ColumnParent parent = new ColumnParent();
	 * parent.setColumn_family(column_family);
	 * predicate.setSlice_range(slice_range);
	 * 
	 * String last_key = "";
	 * 
	 * 
	 * KeyRange range = new KeyRange(); range.setStart_key("");
	 * range.setEnd_key(""); range.setCount(search_slice_ratio);
	 * 
	 * boolean terminated = false;
	 * 
	 * while (!terminated) { List<KeySlice> keys =
	 * getCassandraClient().get_range_slices(keyspace, parent, predicate, range,
	 * ConsistencyLevel.ONE); //delay();
	 * 
	 * if (keys.isEmpty()) {
	 * System.out.println("[INFO|CASSANDRA:]The key range is empty"); } else {
	 * last_key = keys.get(keys.size() - 1).key; range.setStart_key(last_key); }
	 * ColumnPath path = new ColumnPath(); path.setColumn_family(column_family);
	 * for (KeySlice key : keys) { if (!key.columns.isEmpty()) { String key_name
	 * = key.key; //for each client if (!results.containsKey(key_name)) {
	 * results.put(key_name, new TreeMap<String, Map<String, Object>>()); } for
	 * (ColumnOrSuperColumn c : key.getColumns()) { if (c.isSetSuper_column()) {
	 * String superColumn_name = null; //for each item try { superColumn_name =
	 * new String(c.getSuper_column().getName(), "UTF-8"); } catch
	 * (UnsupportedEncodingException e) { e.printStackTrace(); //To change body
	 * of catch statement use File | Settings | File Templates. } if
	 * (!results.get(key_name).containsKey(superColumn_name)) {
	 * results.get(key_name).put(superColumn_name, new TreeMap<String,
	 * Object>()); } for (Column co : c.getSuper_column().columns) {
	 * results.get(key_name).get(superColumn_name).put(new String(co.getName()),
	 * BenchmarkUtil.toObject(co.getValue())); } } } }
	 * 
	 * } if (keys.size() < search_slice_ratio) { terminated = true; } } return
	 * results;
	 * 
	 * 
	 * }
	 */
	/**************************************/
	/**** TPCW benchmark operations ****/
	/**
	 * ********************************
	 */

	// Receives an Costumer id and retrieves is name
	// Receives a item and returns the id and thumbnail of related.
	public void HomeOperation(int customer, int item) throws Exception {

		Bucket bucket = getBucket("item");
		Item itemObj = (Item) GSON.fromJson(bucket.fetch(item + "").execute()
				.getValueAsString(), Item.class);
		bucket = getBucket("customer");
		Customer customerObj = (Customer) GSON.fromJson(
				bucket.fetch(customer + "").execute().getValueAsString(),
				Customer.class);
	}

	/*
	 * 
	 * Adiciona um item com quantidade 1 ao carrinho cria um carrinho novo se
	 * 'create'
	 */

	public void shoppingCartInteraction(int item, boolean create, String cart_id)
			throws Exception {
		ShoppingCart shop_c;
		IRiakObject shop_cObj;
		if (create) {
			Timestamp stamp = new Timestamp(System.currentTimeMillis());
			shop_c = new ShoppingCart(cart_id);
			shop_c.setCartInfo("SC_DATE", stamp.toString());
			shop_cObj = RiakObjectBuilder.newBuilder("shopping_cart", cart_id)
					.build();

		} else {
			Bucket bucket = getBucket("shopping_cart");
			shop_cObj = bucket.fetch(cart_id).execute();
			shop_c = (ShoppingCart) GSON.fromJson(shop_cObj.getValueAsString(),
					ShoppingCart.class);
		}
		Bucket bucket = getBucket("item");
		Item itemObj = (Item) GSON.fromJson(bucket.fetch(item + "").execute()
				.getValueAsString(), Item.class);

		SCLine sc_line = shop_c.getSCLine(item);

		if (sc_line != null) {
			sc_line.setSCL_QTY(sc_line.getSCL_QTY() + 1);
		} else {
			sc_line = new SCLine(item);
			sc_line.setSCL_COST(itemObj.getI_COST());
			sc_line.setSCL_SRP(itemObj.getI_SRP());
			sc_line.setSCL_TITLE(itemObj.getI_TITLE());
			sc_line.setSCL_BACKING(itemObj.getI_BACKING());
			sc_line.setSCL_QTY(1);
			sc_line.setI_ID(itemObj.getI_id());
			shop_c.addSCLine(sc_line);
			// getBucket("sc_lines").store(cart_id + "_" + item,
			// GSON.toJson(sc_line)).execute();
		}

		shop_cObj.setValue(GSON.toJson(shop_c));
		getBucket("shopping_Cart").store(cart_id + "",
				GSON.toJson(shop_c).toString()).execute();

	}

	/*
	 * Registo de utilizador
	 */

	public void CustomerRegistration(String costumer_id) throws Exception {

		String name = (BenchmarkUtil.getRandomAString(8, 13) + " " + BenchmarkUtil
				.getRandomAString(8, 15));
		String[] names = name.split(" ");
		Random r = new Random();
		int random_int = r.nextInt(1000);

		String key = names[0] + "_" + (costumer_id);

		String pass = names[0].charAt(0) + names[1].charAt(0) + "" + random_int;

		String first_name = names[0];

		String last_name = names[1];

		int phone = r.nextInt(999999999 - 100000000) + 100000000;

		String email = key + "@" + BenchmarkUtil.getRandomAString(2, 9)
				+ ".com";

		double discount = r.nextDouble();

		String adress = "Street: "
				+ (BenchmarkUtil.getRandomAString(8, 15) + " " + BenchmarkUtil
						.getRandomAString(8, 15)) + " number: "
				+ r.nextInt(500);

		double C_BALANCE = 0.00;

		double C_YTD_PMT = (double) BenchmarkUtil.getRandomInt(0, 99999) / 100.0;

		GregorianCalendar cal = new GregorianCalendar();
		cal.add(Calendar.DAY_OF_YEAR, -1 * BenchmarkUtil.getRandomInt(1, 730));

		java.sql.Date C_SINCE = new java.sql.Date(cal.getTime().getTime());

		cal.add(Calendar.DAY_OF_YEAR, BenchmarkUtil.getRandomInt(0, 60));
		if (cal.after(new GregorianCalendar())) {
			cal = new GregorianCalendar();
		}

		java.sql.Date C_LAST_LOGIN = new java.sql.Date(cal.getTime().getTime());

		java.sql.Timestamp C_LOGIN = new java.sql.Timestamp(
				System.currentTimeMillis());

		cal = new GregorianCalendar();
		cal.add(Calendar.HOUR, 2);

		java.sql.Timestamp C_EXPIRATION = new java.sql.Timestamp(cal.getTime()
				.getTime());

		cal = BenchmarkUtil.getRandomDate(1880, 2000);
		java.sql.Date C_BIRTHDATE = new java.sql.Date(cal.getTime().getTime());

		String C_DATA = BenchmarkUtil.getRandomAString(100, 500);

		String address_id = insertAdress();

		Customer c = new Customer(costumer_id, key, pass, last_name,
				first_name, phone + "", email, C_SINCE.toString(),
				C_LAST_LOGIN.toString(), C_LOGIN.toString(),
				C_EXPIRATION.toString(), C_BALANCE, C_YTD_PMT,
				C_BIRTHDATE.toString(), C_DATA, discount, address_id);

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

		String key = ADDR_STREET1 + ADDR_STREET2 + ADDR_CITY + ADDR_STATE
				+ ADDR_ZIP + country_id;

		pt.fct.di.benchmarks.TPCW_Riak.entities.Address address = new Address(
				key, ADDR_STREET1, ADDR_STREET2, ADDR_CITY, ADDR_STATE,
				ADDR_ZIP, country_id);

		if (getBucket("address").fetch(key).execute() == null) { // does
			insert(key, "address", address);
		}

		return key;
	}

	public void refreshSession(String C_ID) throws Exception {

		Customer customer = GSON.fromJson(getBucket("customer").fetch(C_ID)
				.execute().getValueAsString(), Customer.class);
		Address address = GSON.fromJson(
				getBucket("address").fetch(customer.getAddress()).execute()
						.getValueAsString(), Address.class);
		customer.setLogin(new Timestamp(System.currentTimeMillis()).toString());
		customer.setExpiration((new Timestamp(
				System.currentTimeMillis() + 7200000)).toString());
		getBucket("customer").store(GSON.toJson(customer).toString()).execute();
	}

	/*
	 * Obtem carrinho Para todas as linhas soma a quantidade e o custo Actualiza
	 * os valores numericos do carrinho
	 */
	public void BuyRequest(String shopping_id) throws
			RiakRetryFailedException, JsonSyntaxException,
			UnresolvedConflictException, ConversionException, JSONException {

		Bucket bucket = getBucket("shopping_cart");
		ShoppingCart shop_c = (ShoppingCart) GSON.fromJson(
				bucket.fetch(shopping_id + "").execute().getValueAsString(),
				ShoppingCart.class);

		int qty = 0;
		float cost = 0;

		Collection<SCLine> scLines = shop_c.getSCLines();
		for (SCLine line : scLines) {

			int qty_read = line.getSCL_QTY();
			qty += qty_read;
			cost += line.getSCL_COST();

		}

		Float SC_SUB_TOTAL = cost * (1 - 0.2f);
		Float SC_TAX = SC_SUB_TOTAL * 0.0825f;
		Float SC_SHIP_COST = 3.00f + (1.00f * qty);
		Float SC_TOTAL = SC_SUB_TOTAL + SC_SHIP_COST + SC_TAX;
		shop_c.setCartInfo("SC_SUB_TOTAL", SC_SUB_TOTAL);// cheats...
		shop_c.setCartInfo("SC_TAX", SC_TAX);
		shop_c.setCartInfo("SC_SHIP_COST", SC_SHIP_COST);
		shop_c.setCartInfo("SC_TOTAL", SC_TOTAL);

		bucket = getBucket("shopping_cart");
		bucket.store(shopping_id + "", GSON.toJson(shop_c).toString())
				.execute();
	}

	/*
	 * Recebe um customer e um carrinho cria um order e um ccxact ccxact
	 * indexado por order_id
	 */
	public void BuyComfirm(String customer, String cart) throws Exception {

		Bucket bucket = getBucket("shopping_cart");
		ShoppingCart shop_c = (ShoppingCart) GSON.fromJson(bucket.fetch(cart)
				.execute().getValueAsString(), ShoppingCart.class);

		bucket = getBucket("customer");
		Customer customerObj = (Customer) GSON.fromJson(bucket.fetch(customer)
				.execute().getValueAsString(), Customer.class);

		double c_discount = (Double) customerObj.getC_DISCOUNT();

		String ship_addr_id = "";
		String cust_addr = (String) customerObj.getAddress();

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
			total = (Float) shop_c.cartInfo.getSC_TOTAL();
		} catch (Exception e) {
			System.out.println("Null pointer on cart:" + cart);
			System.out.println("OPERATIONS: " + operations.toString());
			e.printStackTrace();
			return;
		}

		String order_id = enterOrder(customer, thread_id, shop_c, ship_addr_id,
				cust_addr, shipping, total, c_discount);

		String cc_type = BenchmarkUtil.getRandomAString(10);
		long cc_number = BenchmarkUtil.getRandomNString(16);
		String cc_name = BenchmarkUtil.getRandomAString(30);
		Date cc_expiry = new Date(System.currentTimeMillis()
				+ random.nextInt(644444400));

		enterCCXact(order_id, customer, cc_type, cc_number, cc_name,
				cc_expiry.toString(), total, ship_addr_id.toString());

	}

	/*
	 * Recebe um customer e um shopping cart Cria um order adiciona orderLines
	 * actualiza a quantidade de items vendidos, actualiza a quantidade de
	 * vendidos. Indica que o customer fez um order novo Operaçao pesada
	 */
	public String enterOrder(String customer_id, int thread_id,
			ShoppingCart shoppingCart, String ship_addr_id, String cust_addr,
			String shipping, float total, double c_discount) throws Exception {
		String key = (String) keyGenerator.getNextKey(thread_id);

		float subTotal = (Float) shoppingCart.getCartInfo("SC_SUB_TOTAL");
		float tax = (Float) shoppingCart.getCartInfo("SC_TAX");
		float ship = (Float) shoppingCart.getCartInfo("SC_SHIP_COST");
		String shipType = ship_types[random.nextInt(ship_types.length)];
		String status = status_types[random.nextInt(status_types.length)];

		Order order = new Order(key, customer_id, new Date(
				System.currentTimeMillis()).toString(), subTotal, tax, total,
				shipType, new Date(System.currentTimeMillis()
						+ random.nextInt(604800000)).toString(), status,
				cust_addr, ship_addr_id);

		int index = 0;

		/*
		 * Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		 * Set<String> list = new HashSet<String>();
		 * list.add(shoppingCart.SC_C_ID); map.put("shopping_cart", list);
		 * MapReduceResponse res = getRiakClient().mapReduceOverObjects(map)
		 * .link("sc_lines", "cartline", false)
		 * .map(JavascriptFunction.named("Riak.mapValuesJson"), true) .submit();
		 */

		for (SCLine item : shoppingCart.getSCLines()) {
			int qty = item.getSCL_QTY();
			int item_id = item.getI_ID();

			String ol_key = key + "." + index;

			OrderLine orderLine = new OrderLine(ol_key, key, item_id, qty,
					c_discount, BenchmarkUtil.getRandomAString(20, 100));

			index++;

			order.addOrderLine(orderLine);

			int item_i = orderLine.OL_QTY;
			Item itemFromBucket = GSON.fromJson(
					getBucket("item").fetch(item_id + "").execute()
							.getValueAsString(), Item.class);
			int stock = (Integer) itemFromBucket.getI_STOCK();
			int new_stock = stock - item_i;

			itemFromBucket.setI_STOCK(new_stock);
			itemFromBucket.setI_TOTAL_SOLD(itemFromBucket.getI_TOTAL_SOLD()
					+ item_i);

			BestSellerEntry bse = new BestSellerEntry(itemFromBucket.I_SUBJECT,
					item_id, itemFromBucket.getI_TOTAL_SOLD());

			getBucket("BestSellersBuff").store(item_id + "",
					GSON.toJson(bse).toString()).execute();
			getBucket("item").store(item_id + "",
					GSON.toJson(itemFromBucket).toString());
		}
		Customer customer = (Customer) GSON.fromJson(getBucket("customer")
				.fetch(customer_id).execute().getValueAsString(),
				Customer.class);
		customer.setC_O_LAST_ID(key);

		getBucket("customer").store(customer_id,
				GSON.toJson(customer).toString());
		getBucket("order").store(key, GSON.toJson(order).toString()).execute();
		return customer_id;
	}

	public void enterCCXact(String order_id, String customer, String cc_type,
			long cc_number, String cc_name, String cc_expiry, float total,
			String ship_addr_id) throws Exception {

		Bucket bucket = getBucket("cc_xacts");
		int co_id = (Integer) readfromColumn(ship_addr_id, "address",
				"ADDR_CO_ID", READ_CONSISTENCY_LEVEL);
		CCXactItem ccxactItem = new CCXactItem(cc_type, cc_number, cc_name,
				cc_expiry, total, cc_expiry, order_id, co_id);
		bucket = getBucket("cc_xacts");
		bucket.store(order_id, GSON.toJson(ccxactItem).toString()).execute();

	}

	// TODO: OS fetchs devem estar melhorados com os links
	public void OrderInquiry(String customer) throws Exception {

		Bucket bucket = getBucket("customer");
		IRiakObject obj = bucket.fetch(customer).execute();
		Customer customerObj = (Customer) GSON.fromJson(obj.getValueAsString(),
				Customer.class);
		
		if (customerObj.getC_O_LAST_ID() == null) {
			// System.out.println("Cliente nunca fez orders");
			return;
		} else {
			// System.out.println("Cliente ja fez orders");
		}

		IRiakObject orderRiak = getBucket("order").fetch(
				customerObj.getC_O_LAST_ID()).execute();

		Order orderObj = (Order) GSON.fromJson(orderRiak.getValueAsString(),
				Order.class);

		IRiakObject CC_XACTS_obj = getBucket("cc_xacts").fetch(customer)
				.execute();

		IRiakObject o_bill_addr_id_obj = getBucket("address").fetch(
				orderObj.getO_BILL_ADDR_ID()).execute();
		Address o_bill_addr_id = (Address) GSON.fromJson(
				o_bill_addr_id_obj.getValueAsString(), Address.class);
		IRiakObject o_ship_addr_id_obj = getBucket("address").fetch(
				orderObj.getO_SHIP_ADDR()).execute();
		Address o_ship_addr_id_json = (Address) GSON.fromJson(
				o_ship_addr_id_obj.getValueAsString(), Address.class);
		IRiakObject o_ship_addr_co_id_obj = getRiakIClient()
				.fetchBucket("country").execute()
				.fetch(o_ship_addr_id_json.getCountry_id() + "").execute();
		IRiakObject o_bill_addr_co_id_obj = getRiakIClient()
				.fetchBucket("country").execute()
				.fetch(o_bill_addr_id.getCountry_id() + "").execute();

	}

	/*
	 * Metodo que insere um order sem contexto de utilizaçao, também actualiza o
	 * last order de cada cliente e coloca a venda no buffer
	 */
	public void insertOrder(Order order, List<OrderLine> orderLines,
			CCXactItem ccXact) throws Exception {

		for (OrderLine ol : orderLines) {
			order.addOrderLine(ol);
		}
		getBucket("order")
				.store(order.getO_ID(), GSON.toJson(order).toString())
				.execute();

		for (OrderLine orderLine : orderLines) {
			Item item = GSON.fromJson(
					getBucket("item").fetch(orderLine.getOL_I_ID() + "")
							.execute().getValueAsString(), Item.class);
			int sold = item.getI_TOTAL_SOLD();
			sold += orderLine.OL_QTY;
			item.setI_TOTAL_SOLD(sold);
			// verificar se i_id a null
			BestSellerEntry bse = new BestSellerEntry(item.I_SUBJECT,
					item.I_ID, sold);

			getBucket("BestSellersBuff").store(item.getI_ID() + "",
					GSON.toJson(bse).toString()).execute();
			getBucket("item").store(item.getI_ID() + "",
					GSON.toJson(item).toString()).execute();
		}

		IRiakObject customerRiak = getBucket("customer").fetch(order.O_C_ID)
				.execute();
		Customer customer = (Customer) GSON.fromJson(
				customerRiak.getValueAsString(), Customer.class);
		customer.setC_O_LAST_ID(order.getO_ID());

		getBucket("customer").store(order.O_C_ID,
				GSON.toJson(customer).toString()).execute();

		getBucket("cc_xacts").store(order.getO_ID(), GSON.toJson(ccXact))
				.execute();

	}

	public void doSearch(String term, String field) throws Exception { // 30
		// TODO: Fazer a pesquisa no texto

		if (term.equalsIgnoreCase("SUBJECT")) {
			getBucket(field + "_" + "item_subject_index");

		} else if (term.equalsIgnoreCase("AUTHOR")) {
			getBucket(field + "_" + "item_author_index");

		} else if (term.equalsIgnoreCase("TITLE")) {
			getBucket(field + "_" + "item_title_index");

		} else {
			System.out.println("OPTION NOT RECOGNIZED");
		}
	}

	public void newProducts(String field) throws Exception {
		Iterable<String> it = getRawClient().listKeys(
				field + "_item_subject_index");
		Map<String, Map<String, Object>> results = new HashMap<String, Map<String, Object>>();
		for (String key : it) {
			getBucket("item").fetch(key).execute();
		}
	}

	public Map<String, Map<String, Map<String, Object>>> getResults()
			throws Exception {
		/*
		 * Map<String, Map<String, Object>> results = rangeQuery("results",
		 * null, 10000); // Parece que nos results estao itens e para cada chave
		 * faz-se o // seguinte processamento for (KeySlice key : keys) { if
		 * (!key.columns.isEmpty()) { String key_name = key.key; // for each
		 * client if (!results.containsKey(key_name)) { results.put(key_name,
		 * new TreeMap<String, Map<String, Object>>()); } for
		 * (ColumnOrSuperColumn c : key.getColumns()) { if
		 * (c.isSetSuper_column()) { String superColumn_name = null; // for each
		 * item try { superColumn_name = new String(c.getSuper_column()
		 * .getName(), "UTF-8"); } catch (UnsupportedEncodingException e) {
		 * e.printStackTrace(); // To change body of catch // statement use File
		 * | // Settings | File // Templates. } if (!results.get(key_name)
		 * .containsKey(superColumn_name)) {
		 * results.get(key_name).put(superColumn_name, new TreeMap<String,
		 * Object>()); } for (Column co : c.getSuper_column().columns) {
		 * results.get(key_name) .get(superColumn_name) .put(new
		 * String(co.getName()), BenchmarkUtil.toObject(co .getValue())); } } }
		 * }
		 * 
		 * }
		 * 
		 * return results;
		 */
		return null;
	}

	// Todos os nós processam as mesmas chaves. Errado :( [DISCTUIR]
	public void BestSellers(String subject) throws JsonSyntaxException,
			 RiakException {

		Map<String, BestSellerSubject> subjectBestSellers = new HashMap<String, BestSellerSubject>();
		Bucket bucket = getBucket("BestSellersBuff");
		//TODO: Este varrimento de keys provoca repetições. 
		Iterable<String> it = bucket.keys();
		BestSellerSubject orderedSubjectEntries;
		for (String key : it) {
			IRiakObject objRiak = getBucket("BestSellersBuff").fetch(key)
					.execute();
			if (objRiak == null)
				continue;
			BestSellerEntry bse = GSON.fromJson(objRiak.getValueAsString(),
					BestSellerEntry.class);
			getBucket("BestSellersBuff").delete(key).execute();
			if (!subjectBestSellers.containsKey(bse.I_SUBJECT)) {
				IRiakObject obj = getBucket("bestsellers").fetch(
						bse.getI_SUBJECT()).execute();
				if (obj == null) {
					orderedSubjectEntries = new BestSellerSubject();
				} else {
					orderedSubjectEntries = GSON.fromJson(
							obj.getValueAsString(), BestSellerSubject.class);
				}
				subjectBestSellers.put(bse.I_SUBJECT, orderedSubjectEntries);
			} else {
				orderedSubjectEntries = subjectBestSellers.get(bse.I_SUBJECT);
			}
			List<BestSellerEntry> entries = orderedSubjectEntries.getEntries();
			if (entries.size() == 0) {
				entries.add(bse);
			} else
				for (int i = 0; i < entries.size(); i++) {
					if (entries.get(i).getI_TOTAL_SOLD() <= bse
							.getI_TOTAL_SOLD()) {
						entries.add(i, bse);
						if (entries.size() > BESTSELLERMAX)
							entries.remove(entries.size() - 1);
						break;
					}
					if (i == entries.size() - 1
							&& entries.size() + 1 <= BESTSELLERMAX) {
						entries.add(i + 1, bse);
						break;
					}
				}
		}

		for (Entry<String, BestSellerSubject> entry : subjectBestSellers
				.entrySet()) {
			getBucket("bestsellers").store(entry.getKey(),
					GSON.toJson(entry.getValue())).execute();
		}

	}

	/*
	 * Obtem informações de um item
	 */
	public void ItemInfo(int id) throws Exception {
		IRiakObject obj = getBucket("item").fetch(id + "").execute();
		Item item = GSON.fromJson(obj.getValueAsString(), Item.class);
		IRiakObject obj2 = getBucket("author").fetch(item.I_A_ID + "")
				.execute();
		Author author = GSON.fromJson(obj2.getValueAsString(), Author.class);

	}

	/*
	 * obtem item Actualiza indice (insere este item no indice do subject e
	 * remove o item mais antigo) obtem 5 best sellers entre 10000 itens Altera
	 * atributos do item e armazena
	 */
	public void AdminChange(int item_id) throws Exception {

		Item item = (Item) GSON.fromJson(getBucket("item").fetch(item_id + "")
				.execute().getValueAsString(), Item.class);
		String dateString = item.getI_PUB_DATE();

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		java.util.Date date = formatter.parse(dateString);
		String subject = (String) item.getI_SUBJECT();
		int author_id = (Integer) item.getI_AUTHOR();
		String title = (String) item.getI_TITLE();

		updateIndex(subject, date, item_id, title, author_id);

		Map<String, Map<String, Object>> orders = rangeQuery("order", null,
				10000);

		Map<Integer, Integer> items_info = new TreeMap<Integer, Integer>();

		for (Map<String, Object> orders_info : orders.values()) {
			boolean found = false;
			Collection<OrderLine> orderLines = (Collection<OrderLine>) orders_info
					.get("orderLines");
			TreeMap<Integer, Integer> bought_items = new TreeMap<Integer, Integer>();
			for (OrderLine order_line : orderLines) {
				int i_id = (Integer) order_line.getOL_I_ID();
				if (i_id == item_id) {
					found = true;

				} else {
					int item_qty = (Integer) order_line.getOL_QTY();
					bought_items.put(i_id, item_qty);
				}
			}

			if (found == true) {
				for (Integer i_id : bought_items.keySet()) {
					if (items_info.containsKey(i_id)) {
						int current_qty = items_info.get(i_id);
						items_info.put(i_id,
								(bought_items.get(i_id) + current_qty));
					} else {
						items_info.put(i_id, bought_items.get(i_id));
					}
				}
			}
		}

		Map top_sellers = reverseSortByValue(items_info);

		List<Integer> best = new ArrayList<Integer>();
		int num = 0;
		for (Iterator<Integer> it = top_sellers.keySet().iterator(); it
				.hasNext();) {
			int key = it.next();
			best.add(key);
			num++;
			if (num == 5)
				break;
		}

		if (num < 5) {
			for (int i = num; i < 5; i++) {
				best.add(random.nextInt(990)); // the items are form 0 to 1000
												// right?
			}

		}

		item.setPubDate(new Date(System.currentTimeMillis()).toString());
		item.setI_IMAGE(new String("img" + random.nextInt(1000) % 100
				+ "/image_" + random.nextInt(1000) + ".gif"));
		item.setI_THUMBNAIL(new String("img" + random.nextInt(1000) % 100
				+ "/thumb" + random.nextInt(1000) + ".gif"));
		item.setI_RELATED1(best.get(0));
		item.setI_RELATED2(best.get(1));
		item.setI_RELATED3(best.get(2));
		item.setI_RELATED4(best.get(3));
		item.setI_RELATED5(best.get(4));
		getBucket("item").store(item_id + "", GSON.toJson(item).toString())
				.execute();
	}

	public Object readfromColumn(String key, String ColumnFamily,
			String column, int con) throws Exception {

		IRiakObject bucketObject = getBucket(ColumnFamily).fetch(key).execute();

		if (bucketObject == null)
			return null;
		Entity entity = (Entity) GSON.fromJson(bucketObject.getValueAsString(),
				getEntityClass(ColumnFamily));

		return entity.getValuesToInsert().get(column);

	}

	public void insert(Object value, String key, String column_family,
			String column, int writeConsistency) throws Exception {
		IRiakObject response = getBucket(column_family).fetch(key).execute();
		Entity entity;
		if (response != null && !response.getValue().equals("")) {
			entity = (Entity) GSON.fromJson(response.getValueAsString(),
					getEntityClass(column_family));
		} else {
			Class<?> entityClass = getEntityClass(column_family);
			Constructor<?> ct = entityClass.getConstructor(null);
			entity = (Entity) ct.newInstance(null);
		}
		insertOrModifyAttribute(entity, value, column, writeConsistency);
		getBucket(column_family).fetch(key).execute();
	}

	private Entity insertOrModifyAttribute(Entity entity, Object value,
			String column, int writeConsistency) throws Exception {

		Class<? extends Entity> cls = entity.getClass();
		Field fld = cls.getField(column);
		fld.set(entity, value);
		return entity;
	}

	public IRiakClient getRiakIClient() {
		boolean openClient = false;
		IRiakClient cl = null;

		while (!openClient) { // until there is one open

			if (!iclients.isEmpty()) { // if none, then null...
				cl = iclients.get(last);
				openClient = true;
				last++;
				last = last >= iclients.size() ? 0 : last;
			} else {
				openClient = true;
			}
		}

		return cl;
	}

	public RawClient getRawClient()  {

		boolean openClient = false;
		RawClient cl = null;

		while (!openClient) { // until there is one open

			if (!clients.isEmpty()) { // if none, then null... cl =
				cl = clients.get(lastRaw);
				openClient = true;
				lastRaw++;
				lastRaw = lastRaw >= clients.size() ? 0 : lastRaw;
			} else {
				openClient = true;
			}
		}

		if (cl == null) {
			System.out.println("Exception");
		
		}

		return cl;

	}

	public void modifyOrInsertListOfColumns(String key, String columnFamily,
			Map<String, Object> columnToValuesMap, int level)
			throws Exception {

		Entity entity;
		IRiakObject obj = getBucket(columnFamily).fetch(key).execute();
		if (obj != null) {
			entity = (Entity) GSON.fromJson(obj.getValueAsString(),
					getEntityClass(columnFamily));

		} else {
			entity = (Entity) GSON.fromJson("{}", getEntityClass(columnFamily));
		}

		for (Entry<String, Object> cols : columnToValuesMap.entrySet()) {
			insertOrModifyAttribute(entity, cols.getValue(), cols.getKey(),
					level);
		}

		getBucket(columnFamily).store(key,
				GSON.toJson(entity, getEntityClass(columnFamily)).toString())
				.execute();
	}

	// if columnToValueMap is empty remove key
	public void removeKeyOrColumn(String key, String bucket,
			List<String> columns, int level) throws Exception {

		if (columns == null) {
			getBucket(bucket).delete(key);
		} else {
			// TODO: else nao está feito. Analisar se é preciso fazer
			System.out.println("Nao devia entrar aqui");
			/*
			 * RiakObject obj = getRiakClient().fetch(bucket, key).getObject();
			 * Entity json = (Entity)new Gson().fromJson(obj.getValue(),
			 * getEntityClass(bucket);
			 * 
			 * for (String cols : columns) { json.remove(cols); }
			 * 
			 * obj.setValue(new Gson().toJson(json));
			 * getRiakClient().store(obj);
			 */
		}
	}

	private Class<?> getEntityClass(String string) {
		String className = string.substring(0, 1).toUpperCase()
				+ string.substring(1);
		try {
			return Class
					.forName("pt.fct.di.benchmarks.TPCW_Riak.entities."
							+ className);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	private Bucket getBucket(String bucketName)
			throws RiakRetryFailedException {
		Bucket bucket = getRiakIClient().fetchBucket(bucketName.toLowerCase())
				.execute();
		return bucket;
	}

	/*
	 * Recebe um assunto e um item insere o item no indice do assunto TODO:
	 * [DISCTUIR]Esta funçao devia ser chamada em mais sitios?
	 */
	public void updateIndex(String subject, java.util.Date date, int item_id,
			String title, int author_key) throws Exception {

		Author author = (Author) GSON.fromJson(
				getBucket("author").fetch(author_key + "").execute()
						.getValueAsString(), Author.class);

		AuthorIndex authorIndex = new AuthorIndex(author.getA_FNAME(),
				author.getA_LNAME(), title);

		Date d = new Date(System.currentTimeMillis());
		Long new_time_stamp = Long.MAX_VALUE - d.getTime();
		String new_index_key = new_time_stamp + "." + item_id;
		getBucket(subject + "_item_subject_index").store(new_index_key,
				GSON.toJson(authorIndex)).execute();
		// index(new_index_key, subject+"_item_subject_index", new_index_key,
		// authorIndex);

		// TODO: Isto não deve estar a funcionar, parece estranho
		Long old_time_stamp = Long.MAX_VALUE - date.getTime();
		String old_index_key = old_time_stamp + "." + item_id;
		remove(old_index_key, subject + "_item_subject_index", null);
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

}
