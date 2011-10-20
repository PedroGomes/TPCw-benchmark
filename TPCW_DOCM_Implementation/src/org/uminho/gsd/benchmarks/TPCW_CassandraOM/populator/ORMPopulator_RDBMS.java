/*
 * *********************************************************************
 * Copyright (c) 2010 Pedro Gomes and Universidade do Minho.
 * All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uminho.gsd.benchmarks.TPCW_CassandraOM.populator;


import org.uminho.gsd.benchmarks.TPCW_CassandraOM.entities.*;
import org.uminho.gsd.benchmarks.dataStatistics.ResultHandler;
import org.uminho.gsd.benchmarks.generic.Constants;
import org.uminho.gsd.benchmarks.generic.helpers.NodeKeyGenerator;
import org.uminho.gsd.benchmarks.helpers.BenchmarkUtil;
import org.uminho.gsd.benchmarks.helpers.TestClass;
import org.uminho.gsd.benchmarks.interfaces.Entity;
import org.uminho.gsd.benchmarks.interfaces.executor.AbstractDatabaseExecutorFactory;
import org.uminho.gsd.benchmarks.interfaces.executor.DatabaseExecutorInterface;
import org.uminho.gsd.benchmarks.interfaces.populator.AbstractBenchmarkPopulator;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ORMPopulator_RDBMS extends AbstractBenchmarkPopulator {

	/**
	 * Time measurements
	 */
	private static boolean delay_inserts = true;
	private static int delay_time = 2000;
	private static Random rand = new Random();
	private int rounds = 500;
	private ResultHandler results;
	String result_path;
	static PersistenceManagerFactory pmf;

	//ATTENTION: The NUM_EBS and NUM_ITEMS variables are the only variables
	//that should be modified in order to rescale the DB.
	private static /* final */ int NUM_EBS = Constants.NUM_EBS;
	private static /* final */ int NUM_ITEMS = Constants.NUM_ITEMS;
	private static /* final */ int NUM_CUSTOMERS = Constants.NUM_CUSTOMERS;
	private static /* final */ int NUM_ADDRESSES = Constants.NUM_ADDRESSES;
	private static /* final */ int NUM_AUTHORS = Constants.NUM_AUTHORS;
	private static /* final */ int NUM_ORDERS = Constants.NUM_ORDERS;
	private static /* final */ int NUM_COUNTRIES = Constants.NUM_COUNTRIES; // this is constant. Never changes!

	private static AbstractDatabaseExecutorFactory databaseClientFactory;
	ArrayList<Author> authors = new ArrayList<Author>();
	ArrayList<Address> addresses = new ArrayList<Address>();
	ArrayList<Country> countries = new ArrayList<Country>();
	ArrayList<Customer> costumers = new ArrayList<Customer>();

	ArrayList<Item> items = new ArrayList<Item>();

	boolean debug = true;
	private static int num_threads = 1;
	boolean error = false;
	private CountDownLatch barrier;

	private static boolean client_error = false;

	public ORMPopulator_RDBMS(AbstractDatabaseExecutorFactory database_interface_factory, String conf_filename) {
		super(database_interface_factory, conf_filename);


		pmf = JDOHelper.getPersistenceManagerFactory("datanucleus_relational.properties");

		databaseClientFactory = database_interface_factory;

		Map<String, String> execution_info = configuration.get("BenchmarkPopulator");

		String name = execution_info.get("name");
		if (name == null || name.isEmpty()) {
			name = "TPCW_POPULATOR";
			System.out.println("[WARN:] NO DEFINED NAME: DEFAULT -> TPCW_POPULATOR ");
		}

		String num_threads_info = execution_info.get("thread_number");
		if (num_threads_info == null || num_threads_info.isEmpty()) {
			num_threads = 1;
			System.out.println("[WARN:] NO THREAD NUMBER DEFINED: DEFAULT -> 1");
		} else {
			num_threads = Integer.parseInt(num_threads_info.trim());
		}


		String do_delays = execution_info.get("delay_inserts");
		if (do_delays == null || do_delays.isEmpty()) {
			delay_inserts = false;
			System.out.println("[WARN:] NO DELAY OPTION DEFINED, DELAYS NOT USED");
		} else {
			delay_inserts = Boolean.valueOf(do_delays.trim());
		}


		if (delay_inserts) {
			String delay_time_info = execution_info.get("delay_time");
			if (delay_time_info == null || delay_time_info.isEmpty()) {
				delay_time = 10;
				System.out.println("[WARN:] NO DELAY TIME DEFINED, DEFAULT: 10ms");
			} else {
				delay_time = Integer.valueOf(delay_time_info.trim());
			}
		}


		this.results = new ResultHandler(name, -1);


		result_path = execution_info.get("result_path");
		if (result_path == null || result_path.trim().isEmpty()) {
			result_path = "./results";
		}


		String ebs = execution_info.get("tpcw_numEBS");
		if (ebs != null) {
			Constants.NUM_EBS = Integer.valueOf(ebs.trim());
		} else {
			System.out.println("SCALE FACTOR (EBS) NOT DEFINED. SET TO: " + NUM_EBS);
		}

		String items = execution_info.get("tpcw_numItems");

		if (items != null) {
			Constants.NUM_ITEMS = Integer.valueOf(items.trim());
		} else {
			System.out.println("NUMBER OF ITEMS NOT DEFINED. SET TO: " + NUM_ITEMS);
		}

		Constants.NUM_CUSTOMERS = Constants.NUM_EBS * 2880;
		Constants.NUM_ADDRESSES = 2 * Constants.NUM_CUSTOMERS;
		Constants.NUM_AUTHORS = (int) (.25 * Constants.NUM_ITEMS);
		Constants.NUM_ORDERS = (int) (.9 * Constants.NUM_CUSTOMERS);

		NUM_EBS = Constants.NUM_EBS;
		NUM_ITEMS = Constants.NUM_ITEMS;
		NUM_CUSTOMERS = Constants.NUM_CUSTOMERS;
		NUM_ADDRESSES = Constants.NUM_ADDRESSES;
		NUM_AUTHORS = Constants.NUM_AUTHORS;
		NUM_ORDERS = Constants.NUM_ORDERS;


	}


	public boolean populate() {

		if (error) {
			return false;
		} else {
			try {

				insertShoppingCart();

				insertCountries(NUM_COUNTRIES);
				if (client_error) {
					System.out.println("Error when populating countries");
					return false;
				}
				if (delay_inserts) {
					Thread.sleep(delay_time);
				}

				insertAddresses(NUM_ADDRESSES, true);
				if (client_error) {
					System.out.println("Error when populating addresses");
					return false;
				}
				if (delay_inserts) {
					Thread.sleep(delay_time);
				}


				insertCostumers(NUM_CUSTOMERS);
				if (client_error) {
					System.out.println("Error when populating costumers");
					return false;
				}
				if (delay_inserts) {
					Thread.sleep(delay_time);
				}


				insertAuthors(NUM_AUTHORS, true);
				if (client_error) {
					System.out.println("Error when populating authors");
					return false;
				}
				if (delay_inserts) {
					Thread.sleep(delay_time);
				}

				insertItems(NUM_ITEMS);
				if (client_error) {
					System.out.println("Error when populating items");
					return false;
				}
				if (delay_inserts) {
					Thread.sleep(delay_time);
				}
				insertOrder_and_CC_XACTS(NUM_ORDERS);

				if (client_error) {
					System.out.println("Error when populating orders");
					return false;
				}


				System.out.println("***Finished***");


			} catch (InterruptedException ex) {
				Logger.getLogger(ORMPopulator_RDBMS.class.getName()).log(Level.SEVERE, null, ex);
				return false;
			} catch (Exception ex) {
				Logger.getLogger(ORMPopulator_RDBMS.class.getName()).log(Level.SEVERE, null, ex);
				return false;
			}

			results.listDataToSOutput();
			results.listDatatoFiles(result_path, "", true);
			results.cleanResults();
			return true;
		}
	}

	public void cleanDB() throws Exception {
		removeALL();
	}

	public void BenchmarkClean() throws Exception {
		DatabaseExecutorInterface client = databaseClientFactory.getDatabaseClient();
		client.truncate("ShoppingCart");
		client.truncate("ShoppingCartLine");
		client.truncate("Order");
		client.truncate("OrderLine");
		client.truncate("CCXact");


//        client.truncate("Results");
//        client.truncate("Items");
//        try {
//            insertAuthors(NUM_AUTHORS, false);
//            insertItems(NUM_ITEMS);
//        } catch (InterruptedException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
		client.closeClient();

	}

	public void removeALL() throws Exception {

		DatabaseExecutorInterface client = databaseClientFactory.getDatabaseClient();

		client.truncate("ShoppingCart");
		client.truncate("ShoppingCartLine");

		client.truncate("Order");
		client.truncate("OrderLine");
		client.truncate("CCXact");

		client.truncate("Customer");
		client.truncate("Address");
		client.truncate("Country");


		client.truncate("Item");
		client.truncate("Author");
		//  client.truncate("Results");
		client.closeClient();
	}

	public void databaseInsert(DatabaseExecutorInterface client, String Operation, String key, String path, Entity value, ResultHandler results) throws Exception {


		PersistenceManager pm = pmf.getPersistenceManager();
		long time1 = System.currentTimeMillis();

		try {


			pm.makePersistent(value);

		} finally {
			pm.close();
		}

		long time2 = System.currentTimeMillis();
		results.logResult(Operation, time2 - time1);

	}


	/************************************************************************/
	/************************************************************************/
	/************************************************************************/


	//Inserted and removed for automatic schema creation.

	/**
	 * **************
	 * Shopping Cart *
	 * **************
	 */

	public void insertShoppingCart() {

		PersistenceManager pm = pmf.getPersistenceManager();

		LinkedList item_list = new LinkedList<Integer>();
		item_list.add(-1);
		item_list.add(-2);
		item_list.add(-3);
		item_list.add(-4);
		item_list.add(-5);


		Item item = new Item(-1, "iii", new java.sql.Date(1), "", "", "fff", "", "", 1, 1, "", 2, item_list, 1, new Date(1), "", "", new Author(-1, "", "", "", new Date(1), ""));

		ShoppingCart cart_aux = new ShoppingCart("-1");
		cart_aux.setSC_DATE(new Timestamp(new GregorianCalendar().getTimeInMillis()));
		pm.makePersistent(cart_aux);

		ShoppingCartLine cartLine = new ShoppingCartLine("-1", item, 1);
		String scl_id = cartLine.getShoppingCartLineID();
		pm.makePersistent(cartLine);

		cartLine = pm.getObjectById(ShoppingCartLine.class,scl_id);
		pm.deletePersistent(cartLine);

		cart_aux = pm.getObjectById(ShoppingCart.class,"-1");
		pm.deletePersistent(cart_aux);

		item = pm.getObjectById(Item.class,-1);
		pm.deletePersistent(item);

		Author author = pm.getObjectById(Author.class,-1);
		pm.deletePersistent(author);

		Order order = new Order("-1",null,new Date(0),-1,-1,-1,"",new Date(0),"","","");
		pm.makePersistent(order);

		order = pm.getObjectById(Order.class,"-1");
		pm.deletePersistent(order);

	}


	/**
	 * ************
	 * Authors*
	 * **************
	 */

	public void insertAuthors(int n, boolean insert) throws InterruptedException {
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

		System.out.println(">>Inserting " + n + " Authors || populatores " + num_threads);
		barrier = new CountDownLatch(threads);

		AuthorPopulator[] partial_authors = new AuthorPopulator[threads];
		for (int i = threads; i > 0; i--) {

			int base = (threads - i) * sections;

			AuthorPopulator populator = null;
			if (i == 0) {
				populator = new AuthorPopulator(firstSection, base, insert);

			} else {
				populator = new AuthorPopulator(sections, base, insert);
			}
			partial_authors[threads - i] = populator;
			Thread t = new Thread(populator);
			t.start();
		}

		barrier.await();
		for (AuthorPopulator populator : partial_authors) {
			ArrayList<Author> ids = populator.getData();
			for (Author author : ids) {
				authors.add(author);
			}
			if (insert)
				results.addResults(populator.returnResults());
			populator.partial_results.cleanResults();

		}
		partial_authors = null;
		System.gc();

	}

	class AuthorPopulator implements Runnable {

		int num_authors;
		DatabaseExecutorInterface client;
		ArrayList<Author> partial_authors;
		ResultHandler partial_results;
		boolean insertDB;
		int base;

		public AuthorPopulator(int num_authors, int base, boolean insertDB) {
			client = databaseClientFactory.getDatabaseClient();
			this.num_authors = num_authors;
			partial_authors = new ArrayList<Author>();
			partial_results = new ResultHandler("", rounds);
			this.insertDB = insertDB;
			this.base = base;
		}

		public void run() {
			this.insertAuthors(num_authors);
		}

		public void databaseInsert(String Operation, String key, String path, Entity value, ResultHandler results) throws Exception {

			long time1 = System.currentTimeMillis();
			client.insert(key, path, value);
			long time2 = System.currentTimeMillis();
			results.logResult(Operation, time2 - time1);
		}

		public void insertAuthors(int n) {


			System.out.println("Inserting Authors: base= " + base + " total=" + n);

			for (int i = 0; i < n; i++) {

				if(base+i==500){
					System.out.println("DD:"+ base + ":"+n );
				}

				GregorianCalendar cal = BenchmarkUtil.getRandomDate(1800, 1990);

				String[] names = (BenchmarkUtil.getRandomAString(3, 20) + " " + BenchmarkUtil.getRandomAString(2, 20)).split(" ");
				String[] Mnames = ("d " + BenchmarkUtil.getRandomAString(2, 20)).split(" ");

				String first_name = names[0];
				String last_name = names[1];
				String middle_name = Mnames[1];
				Date dob = new Date(cal.getTime().getTime());
				String bio = BenchmarkUtil.getRandomAString(125, 256);

//            insert(first_name, key, "Author", "A_FNAME", writeConsistency);
//            insert(last_name, key, "Author", "A_LNAME", writeConsistency);
//            insert(middle_name, key, "Author", "A_MNAME", writeConsistency);
//            insert(dob, key, "Author", "A_DOB", writeConsistency);
//            insert(bio, key, "Author", "A_BIO", writeConsistency);

				Author a = new Author((base + i), first_name, last_name, middle_name, dob, bio);
				if (insertDB)
					try {
						databaseInsert("INSERT_Authors", (base + i) + "", "Author", a, partial_results);
					} catch (Exception e) {
						e.printStackTrace();
						client_error = true;

						break;
					}

				partial_authors.add(a);
			}
			if (debug) {
				System.out.println("Thread finished: " + num_authors + " authors inserted");
			}

			barrier.countDown();
			client.closeClient();

		}

		public ArrayList<Author> getData() {
			return partial_authors;
		}

		public ResultHandler returnResults() {
			return partial_results;
		}
	}


	/**
	 * ************
	 * Costumers*
	 * **************
	 */
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
		System.out.println(">>Inserting " + n + " Costumers || populatores " + num_threads);
		barrier = new CountDownLatch(threads);

		CostumerPopulator[] partial_costumers = new CostumerPopulator[threads];
		for (int i = threads; i > 0; i--) {

			int base = (threads - i) * sections;

			CostumerPopulator populator = null;
			if (i == 0) {
				populator = new CostumerPopulator(firstSection, base);

			} else {
				populator = new CostumerPopulator(sections, base);
			}
			partial_costumers[threads - i] = populator;
			Thread t = new Thread(populator);
			t.start();
		}
		barrier.await();
		for (CostumerPopulator populator : partial_costumers) {
			ArrayList<Customer> ids = populator.getData();
			for (Customer costumer : ids) {
				costumers.add(costumer);
			}
			results.addResults(populator.returnResults());
			populator.partial_results.cleanResults();

		}
		partial_costumers = null;
		System.gc();

	}


	class CostumerPopulator implements Runnable {

		DatabaseExecutorInterface client;
		int num_costumers;
		ArrayList<Customer> partial_costumers;
		ResultHandler partial_results;
		int base;

		public CostumerPopulator(int num_costumers, int base) {
			client = databaseClientFactory.getDatabaseClient();
			this.num_costumers = num_costumers;
			partial_costumers = new ArrayList<Customer>();
			partial_results = new ResultHandler("", rounds);
			this.base = base;

		}

		public void run() {
			this.insertCostumers(num_costumers);
		}

		public void insertCostumers(int n) {


			System.out.println("Inserting Costumers: " + n);
			for (int i = 0; i < n; i++) {

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


				double C_BALANCE = 0.00;
				//   insert(C_BALANCE, key, "Customer", "C_BALANCE", writeCon);

				double C_YTD_PMT = (double) BenchmarkUtil.getRandomInt(0, 99999) / 100.0;
				//   insert(C_YTD_PMT, key, "Customer", "C_YTD_PMT", writeCon);

				GregorianCalendar cal = new GregorianCalendar();
				cal.add(Calendar.DAY_OF_YEAR, -1 * BenchmarkUtil.getRandomInt(1, 730));

				Date C_SINCE = new Date(cal.getTime().getTime());
				//  insert(C_SINCE, key, "Customer", "C_SINCE ", writeCon);

				cal.add(Calendar.DAY_OF_YEAR, BenchmarkUtil.getRandomInt(0, 60));
				if (cal.after(new GregorianCalendar())) {
					cal = new GregorianCalendar();
				}

				Date C_LAST_LOGIN = new Date(cal.getTime().getTime());
				//insert(C_LAST_LOGIN, key, "Customer", "C_LAST_LOGIN", writeCon);

				Timestamp C_LOGIN = new Timestamp(System.currentTimeMillis());
				//insert(C_LOGIN, key, "Customer", "C_LOGIN", writeCon);

				cal = new GregorianCalendar();
				cal.add(Calendar.HOUR, 2);

				Timestamp C_EXPIRATION = new Timestamp(cal.getTime().getTime());
				//insert(C_EXPIRATION, key, "Customer", "C_EXPIRATION", writeCon);

				cal = BenchmarkUtil.getRandomDate(1880, 2000);
				Date C_BIRTHDATE = new Date(cal.getTime().getTime());
				//insert(C_BIRTHDATE, key, "Customer", "C_BIRTHDATE", writeCon);

				String C_DATA = BenchmarkUtil.getRandomAString(100, 256);
				//insert(C_DATA, key, "Customer", "C_DATA", writeCon);

				Address address = addresses.get(rand.nextInt(addresses.size()));
				//insert(address.getAddr_id(), key, "Customer", "C_ADDR_ID", writeCon);

				Customer c = new Customer("0." + (base + i) + "", key, pass, last_name, first_name, phone, email, C_SINCE, C_LAST_LOGIN, C_LOGIN, C_EXPIRATION, C_BALANCE, C_YTD_PMT, C_BIRTHDATE, C_DATA, discount, address);

				try {
					databaseInsert("INSERT_Costumers", "0." + (base + i) + "", "Customer", c, partial_results);
				} catch (Exception e) {
					e.printStackTrace();
					client_error = true;
					break;
				}


				partial_costumers.add(c);


			}
			if (debug) {
				System.out.println("Thread finished: " + num_costumers + " costumers inserted");
			}
			barrier.countDown();
			client.closeClient();
		}

		public ArrayList<Customer> getData() {
			return partial_costumers;
		}

		public ResultHandler returnResults() {
			return partial_results;
		}


		public void databaseInsert(String Operation, String key, String path, Customer value, ResultHandler results) throws Exception {

			String addr_id = value.getAddress().getAddr_id();

			PersistenceManager pm = pmf.getPersistenceManager();
			long time1 = System.currentTimeMillis();

			try {
				try {

					Address addr = pm.getObjectById(Address.class, addr_id);
					value.setAddress(addr);
				} catch (javax.jdo.JDOObjectNotFoundException e) {

					throw e;
//
				}

				pm.makePersistent(value);
				long time2 = System.currentTimeMillis();
				results.logResult(Operation, time2 - time1);

			} finally {
				pm.close();
			}


		}

	}

	/**
	 * ************
	 * Items*
	 * **************
	 */
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

		System.out.println(">>Inserting " + n + " Items || populatores " + num_threads);
		barrier = new CountDownLatch(threads);

		ItemPopulator[] partial_items = new ItemPopulator[threads];
		for (int i = threads; i > 0; i--) {

			int base = (threads - i) * sections;

			ItemPopulator populator = null;
			if (i == 0) {
				populator = new ItemPopulator(firstSection, base);

			} else {
				populator = new ItemPopulator(sections, base);
			}
			partial_items[threads - i] = populator;
			Thread t = new Thread(populator);
			t.start();
		}
		barrier.await();

		for (ItemPopulator populator : partial_items) {
			ArrayList<Item> ids = populator.getData();
			for (Item item : ids) {
				items.add(item);
			}
			results.addResults(populator.returnResults());
			populator.partial_results.cleanResults();
		}
		partial_items = null;
		System.gc();

	}

	class ItemPopulator implements Runnable {

		DatabaseExecutorInterface client;
		int num_items;
		ArrayList<Item> partial_items;
		ResultHandler partial_results;
		int base;

		public ItemPopulator(int num_items, int base) {
			client = databaseClientFactory.getDatabaseClient();
			this.num_items = num_items;
			partial_items = new ArrayList<Item>();
			partial_results = new ResultHandler("", rounds);
			this.base = base;
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


			String column_family = "Items";

			System.out.println("Inserting Items: " + n);

			ArrayList<String> titles = new ArrayList<String>();
			for (int i = 0; i < n; i++) {

				String f_name = BenchmarkUtil.getRandomAString(14, 60);
				int num = rand.nextInt(1000);
				titles.add(f_name + " " + num);
			}

			for (int i = 0; i < n; i++) {


				String I_TITLE; //ID
				Author I_AUTHOR;
				String I_PUBLISHER;
				String I_DESC;
				String I_SUBJECT;
				float I_COST;
				long I_STOCK;
				List<Integer> I_RELATED = new ArrayList<Integer>();
				int I_PAGE;
				String I_BACKING;
				I_TITLE = titles.get(i);


				int author_pos = rand.nextInt(authors.size());
				Author author = authors.get(author_pos);


				I_AUTHOR = author;//(BenchmarkUtil.getRandomAString(8, 15) + " " + BenchmarkUtil.getRandomAString(8, 15));
				//     insert(I_AUTHOR, I_TITLE, column_family, "I_AUTHOR", writeCon);


				I_PUBLISHER = BenchmarkUtil.getRandomAString(14, 60);
				//    insert(I_PUBLISHER, I_TITLE, column_family, "I_PUBLISHER", writeCon);

				boolean rad1 = rand.nextBoolean();
				I_DESC = null;
				if (rad1) {
					I_DESC = BenchmarkUtil.getRandomAString(100, 256);
					//      insert(I_DESC, I_TITLE, column_family, "I_DESC", writeCon);
				}

				I_COST = rand.nextInt(100);
				// insert(I_AUTHOR, I_TITLE, column_family, "I_AUTHOR", writeCon);

				I_STOCK = BenchmarkUtil.getRandomInt(10, 30);
				// insert(I_STOCK, I_TITLE, column_family, "I_STOCK", writeCon);


				for (int z = 0; z < 5; z++) {
					I_RELATED.add(rand.nextInt(n));
				}


				I_PAGE = rand.nextInt(500) + 10;
				//  insert(I_PAGE, I_TITLE, column_family, "I_PAGE", writeCon);

				I_SUBJECT = subjects[rand.nextInt(subjects.length - 1)];
				// insert(I_SUBJECT, I_TITLE, column_family, "I_SUBJECT", writeCon);

				I_BACKING = backings[rand.nextInt(backings.length - 1)];
				//insert(I_BACKING, I_TITLE, column_family, "I_BACKING", writeCon);


				GregorianCalendar cal = BenchmarkUtil.getRandomDate(1930, 2000);

				Date pubDate = new Date(System.currentTimeMillis());


				String thumbnail = new String("img" + i % 100 + "/thumb_" + i + ".gif");
				String image = new String("img" + i % 100 + "/image_" + i + ".gif");

				double srp = (double) BenchmarkUtil.getRandomInt(100, 99999);
				srp /= 100.0;

				String isbn = BenchmarkUtil.getRandomAString(13);

				Date avail = new Date(System.currentTimeMillis() + rand.nextInt(1200000)); //Data when available

				String dimensions = ((double) BenchmarkUtil.getRandomInt(1, 9999) / 100.0) + "x"
						+ ((double) BenchmarkUtil.getRandomInt(1, 9999) / 100.0) + "x"
						+ ((double) BenchmarkUtil.getRandomInt(1, 9999) / 100.0);


				Item item = new Item(base + i, I_TITLE, pubDate, I_PUBLISHER, I_DESC, I_SUBJECT, thumbnail, image, I_COST, I_STOCK, isbn, srp, I_RELATED, I_PAGE, avail, I_BACKING, dimensions, author);

				try {
					databaseInsert("INSERT_Items", (base + i) + "", column_family, item, partial_results);
				} catch (Exception e) {
					e.printStackTrace();
					client_error = true;

					break;
				}

				partial_items.add(item);

			}
			if (debug) {
				System.out.println("Thread finished: " + num_items + " items inserted");
			}

			barrier.countDown();
			client.closeClient();
		}

		public void databaseInsert(String Operation, String key, String path, Item value, ResultHandler results) throws Exception {

			PersistenceManager pm = pmf.getPersistenceManager();

			int author_id = value.getI_AUTHOR().getA_id();


			long time1 = System.currentTimeMillis();
			try {
				Author author = pm.getObjectById(Author.class, author_id);

				value.setI_AUTHOR(author);


				pm.makePersistent(value);


			} finally {
				pm.close();
			}

			long time2 = System.currentTimeMillis();
			results.logResult(Operation, time2 - time1);

		}

		public ArrayList<Item> getData() {
			return partial_items;
		}

		public ResultHandler returnResults() {
			return partial_results;
		}
	}

	/**
	 * ***********
	 * Addresses*
	 * ***********
	 */
	public void insertAddresses(int n, boolean insert) throws InterruptedException {

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

		System.out.println(">>Inserting " + n + " Addresses || populatores " + num_threads);

		barrier = new CountDownLatch(threads);
		AddressPopulator[] partial_addresses = new AddressPopulator[threads];
		for (int i = threads; i > 0; i--) {

			int base = (threads - i) * sections;

			AddressPopulator populator = null;
			if (i == 0) {
				populator = new AddressPopulator(firstSection, insert, base);

			} else {
				populator = new AddressPopulator(sections, insert, base);
			}
			Thread t = new Thread(populator);
			partial_addresses[threads - i] = populator;
			t.start();
		}
		barrier.await();

		for (AddressPopulator populator : partial_addresses) {

			ArrayList<Address> ids = populator.getData();
			for (Address address : ids) {
				addresses.add(address);
			}
			if (insert)
				results.addResults(populator.returnResults());
			populator.partial_results.cleanResults();
			populator = null;
		}
		partial_addresses = null;
		System.gc();

	}

	class AddressPopulator implements Runnable {

		int num_addresses;
		DatabaseExecutorInterface client;
		ArrayList<Address> partial_adresses;
		ResultHandler partial_results;
		boolean insertDB;
		int base;

		public AddressPopulator(int num_addresses, boolean insertDB, int base) {
			client = databaseClientFactory.getDatabaseClient();
			this.num_addresses = num_addresses;
			partial_adresses = new ArrayList<Address>();
			partial_results = new ResultHandler("", rounds);
			this.insertDB = insertDB;
			this.base = base;
		}

		public void run() {
			this.insertAddress(num_addresses);
		}

		public void databaseInsert(String Operation, String key, String path, Address value, ResultHandler results) throws Exception {

			long time1 = System.currentTimeMillis();

			int id = value.getCountry().getCo_id();


			PersistenceManager pm = pmf.getPersistenceManager();
			try {


				Country c = pm.getObjectById(Country.class, id);
				value.setCountry(c);

				if (value.getStreet1() == null || value.getStreet1().isEmpty()) {
					System.out.println("INSERTING NULL VALUES: " + value.toString());
				}

				pm.makePersistent(value);


			} finally {
				pm.close();
			}

			long time2 = System.currentTimeMillis();
			results.logResult(Operation, time2 - time1);

		}

		private void insertAddress(int n) {

			System.out.println("Inserting Address: " + n);

			String ADDR_STREET1, ADDR_STREET2, ADDR_CITY, ADDR_STATE;
			String ADDR_ZIP;
			Country country_id;

			for (int i = 0; i < n; i++) {
				ADDR_STREET1 = "street" + BenchmarkUtil.getRandomAString(10, 30);


				ADDR_STREET2 = "street" + BenchmarkUtil.getRandomAString(10, 30);
				ADDR_CITY = BenchmarkUtil.getRandomAString(4, 30);
				ADDR_STATE = BenchmarkUtil.getRandomAString(2, 20);
				ADDR_ZIP = BenchmarkUtil.getRandomAString(5, 10);
				country_id = countries.get(BenchmarkUtil.getRandomInt(0, NUM_COUNTRIES - 1));


				String key = ADDR_STREET1 + ADDR_STREET2 + ADDR_CITY + ADDR_STATE + ADDR_ZIP + country_id;

				Address address = new Address(key, ADDR_STREET1, ADDR_STREET2, ADDR_CITY,
						ADDR_STATE, ADDR_ZIP, country_id);
//            insert(ADDR_STREET1, key, "Addresses", "ADDR_STREET1", writeConsistency);
//            insert(ADDR_STREET2, key, "Addresses", "ADDR_STREET2", writeConsistency);
//            insert(ADDR_STATE, key, "Addresses", "ADDR_STATE", writeConsistency);
//            insert(ADDR_CITY, key, "Addresses", "ADDR_CITY", writeConsistency);
//            insert(ADDR_ZIP, key, "Addresses", "ADDR_ZIP", writeConsistency);
//            insert(country.getCo_id(), key, "Addresses", "ADDR_CO_ID", writeConsistency);

				if (insertDB) {
					try {
						databaseInsert("INSERT_Addresses", (base + i) + "", "Addresses", address, partial_results);
					} catch (Exception e) {
						e.printStackTrace();
						client_error = true;

						break;
					}
				}
				partial_adresses.add(address);


			}
			if (debug) {
				System.out.println("Thread finished: " + num_addresses + " addresses.");
			}

			barrier.countDown();
			client.closeClient();
		}

		public ArrayList<Address> getData() {
			return partial_adresses;
		}

		public ResultHandler returnResults() {
			return partial_results;
		}
	}

	/**
	 * ********
	 * Countries *
	 * *********
	 */
	private void insertCountries(int numCountries) {

		DatabaseExecutorInterface client;
		client = databaseClientFactory.getDatabaseClient();

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


		System.out.println(">>Inserting " + numCountries + " countries || populatores " + num_threads);


		for (int i = 0; i < numCountries; i++) {

			//Country name = key
			//insert(exchanges[i], countriesNames[i], "Countries", "CO_EXCHANGE", writeConsitency);
			//insert(currencies[i], countriesNames[i], "Countries", "CO_CURRENCY", writeConsitency);
			Country country = new Country(i, countriesNames[i], currencies[i], exchanges[i]);


//            final URL location;
//            final String classLocation = country.getClass().getName().replace('.', '/')
//                    + ".class";
//            final ClassLoader loader = country.getClass().getClassLoader();
//            if (loader == null) {
//                System.out.println("Cannot load the class");
//            } else {
//                location = loader.getResource(classLocation);
//                System.out.println("County class Class " + location);
//            }

			try {
				databaseInsert(client, "INSERT_Countries", i + "", "Countries", country, results);
			} catch (Exception e) {
				e.printStackTrace();
				client_error = true;

				break;
			}
			this.countries.add(country);
		}
		if (debug) {
			System.out.println("Countries:" + countriesNames.length + " inserted");
		}
	}


	/**
	 * ****************
	 * Order and XACTS *
	 * ******************
	 */
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

		System.out.println(">>Inserting " + n + " Order .. || populatores " + num_threads);


		barrier = new CountDownLatch(threads);

		Order_and_XACTSPopulator[] partial_orders = new Order_and_XACTSPopulator[threads];
		for (int i = 0; i < threads; i++) {

			int base = (threads - i);


			Order_and_XACTSPopulator populator = null;
			if (i == 0) {
				populator = new Order_and_XACTSPopulator(firstSection, base);

			} else {
				populator = new Order_and_XACTSPopulator(sections, base);
			}
			partial_orders[i] = populator;
			Thread t = new Thread(populator);
			t.start();
		}
		barrier.await();

		for (Order_and_XACTSPopulator populator : partial_orders) {
			results.addResults(populator.returnResults());
			populator.partial_results.cleanResults();
			populator = null;
		}
		System.gc();

	}

	class Order_and_XACTSPopulator implements Runnable {

		int num_orders;
		int base = 0;
		DatabaseExecutorInterface client;
		ResultHandler partial_results;

		public Order_and_XACTSPopulator(int num_orders, int base
		) {
			client = databaseClientFactory.getDatabaseClient();
			this.num_orders = num_orders;
			partial_results = new ResultHandler("", rounds);
			this.base = base;
		}

		public void run() {
			this.insertOrder_and_CC_XACTS(num_orders);
		}

		public void databaseInsert(Order order, List<OrderLine> orderLines, CCXact ccXact) throws Exception {

			PersistenceManager pm = pmf.getPersistenceManager();

			try {


				String costumer_id = order.getO_C().getC_id();

				Customer cust = null;
				try {

					cust = pm.getObjectById(Customer.class, costumer_id);

				} catch (javax.jdo.JDOObjectNotFoundException e) {
					System.out.println("Customer not found: " + costumer_id);
					TestClass.getData("Customer", costumer_id);
					throw e;

				}

				order.setO_C(cust);

				for (OrderLine orderLine : orderLines) {
					Item item = null;
					try {


						item = pm.getObjectById(Item.class, orderLine.getOL_I_ID().getI_id());
					} catch (javax.jdo.JDOObjectNotFoundException e) {
						System.out.println("Cant find key :" + orderLine.getOL_I_ID().getI_id());
						e.printStackTrace();
						throw e;

					}

					orderLine.setOL_I_ID(item);
					pm.makePersistent(orderLine);
				}
				order.setOrderlines(orderLines);
				pm.makePersistent(order);

				pm.makePersistent(ccXact);



			} finally {

				pm.close();
			}


		}

		public void insertOrder_and_CC_XACTS(int number_keys) {
//
//
			System.out.println("Inserting Order: " + number_keys);
			String table = "Order";
			String[] credit_cards = {"VISA", "MASTERCARD", "DISCOVER", "AMEX", "DINERS"};
			String[] ship_types = {"AIR", "UPS", "FEDEX", "SHIP", "COURIER", "MAIL"};
			String[] status_types = {"PROCESSING", "SHIPPED", "PENDING", "DENIED"};
//
//            long O_ID = begin_key;
//            // ColumnPath path = new ColumnPath(column_family);
//            // path.setSuper_column("ids".getBytes());
//
			NodeKeyGenerator nodeKeyGenerator = new NodeKeyGenerator(0);

			for (int z = 0; z < number_keys; z++) {

				table = "ORDERS";

				Customer O_C_ID;
				Date O_DATE;
				float O_SUB_TOTAL;
				float O_TAX;
				float O_TOTAL;
				Date O_SHIP_DATE;
				String O_SHIP_TYPE;
				Address O_SHIP_ADDR;
				String O_STATUS;


				Customer Customer_id = costumers.get(rand.nextInt(costumers.size()));

				O_C_ID = Customer_id;

				GregorianCalendar call = new GregorianCalendar();
				O_DATE = new Date(call.getTime().getTime());
				//insertInSuperColumn(O_DATE, O_C_ID, column_family, O_ID + "", "O_DATE", write_con);

				O_SUB_TOTAL = rand.nextFloat() * 100 * 4;
				//insertInSuperColumn(O_SUB_TOTAL, O_C_ID, column_family, O_ID + "", "O_SUB_TOTAL", write_con);

				O_TAX = O_SUB_TOTAL * 0.21f;
				//insertInSuperColumn(O_TAX, O_C_ID, column_family, O_ID + "", "O_TAX", write_con);

				O_TOTAL = O_SUB_TOTAL + O_TAX;
				//insertInSuperColumn(O_TOTAL, O_C_ID, column_family, O_ID + "", "O_TOTAL", write_con);

				call.add(Calendar.DAY_OF_YEAR, -1 * rand.nextInt(60) + 1);
				O_SHIP_DATE = new Date(call.getTime().getTime());
				//insertInSuperColumn(O_SHIP_DATE, O_C_ID, column_family, O_ID + "", "O_SHIP_DATE", write_con);

				O_SHIP_TYPE = ship_types[rand.nextInt(ship_types.length)];
				//insertInSuperColumn(O_SHIP_TYPE, O_C_ID, column_family, O_ID + "", "O_SHIP_TYPE", write_con);

				O_STATUS = status_types[rand.nextInt(status_types.length)];
				//insertInSuperColumn(O_STATUS, O_C_ID, column_family, O_ID + "", "O_STATUS", write_con);

				Address billAddress = addresses.get(BenchmarkUtil.getRandomInt(0, NUM_ADDRESSES - 1));
				// insertInSuperColumn(billAddress.getAddr_id(), O_C_ID, column_family, O_ID + "", "O_BILL_ADDR_ID", write_con);

				O_SHIP_ADDR = addresses.get(BenchmarkUtil.getRandomInt(0, NUM_ADDRESSES - 1));
				// insertInSuperColumn(O_SHIP_ADDR.getAddr_id(), O_C_ID, column_family, O_ID + "", "O_SHIP_ADDR_ID", write_con);

				String order_id = (String) nodeKeyGenerator.getNextKey(base);

				Order order = new Order(order_id, O_C_ID, O_DATE, O_SUB_TOTAL, O_TAX, O_TOTAL, O_SHIP_TYPE, O_SHIP_DATE, O_STATUS, billAddress.getAddr_id(), O_SHIP_ADDR.getAddr_id());

				//orders.add(order);
//
//
				int number_of_items = rand.nextInt(4) + 1;
//
				List<OrderLine> orderLines = new ArrayList<OrderLine>();

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

					Item OL_I_ID;
					int OL_QTY;
					float OL_DISCOUNT;
					String OL_COMMENT;

					OL_ID = order_id + "." + i;


					OL_I_ID = items.get(rand.nextInt(items.size()));

					OL_QTY = rand.nextInt(4) + 1;

					OL_DISCOUNT = (float) rand.nextInt(30) / 100f;

					OL_COMMENT = null;

					OL_COMMENT = BenchmarkUtil.getRandomAString(20, 100);

					OrderLine orderline = new OrderLine(OL_ID, OL_I_ID, OL_QTY, OL_DISCOUNT, OL_COMMENT);
					orderLines.add(orderline);
				}
//


//
				String CX_TYPE;
				int CX_NUM;
				String CX_NAME;
				Date CX_EXPIRY;
				double CX_XACT_AMT;
				int CX_CO_ID; //Order.getID;

				table = "CC_XACTS";

				CX_NUM = BenchmarkUtil.getRandomNString(16);
				int key = base + z;

				CX_TYPE = credit_cards[BenchmarkUtil.getRandomInt(0, credit_cards.length - 1)];


				CX_NAME = BenchmarkUtil.getRandomAString(14, 30);
				//insert(CX_NAME, key, column_family, "CX_NAME", write_con);

				GregorianCalendar cal = new GregorianCalendar();
				cal.add(Calendar.DAY_OF_YEAR, BenchmarkUtil.getRandomInt(10, 730));
				CX_EXPIRY = new Date(cal.getTime().getTime());

				Country country_id = countries.get(BenchmarkUtil.getRandomInt(0, countries.size() - 1));


				CCXact ccXact = new CCXact(CX_TYPE, CX_NUM, CX_NAME, CX_EXPIRY,/* CX_AUTH_ID,*/ O_TOTAL,
						O_SHIP_DATE, /* 1 + _counter, */ order.getO_ID(), country_id.getName());

				try {
					databaseInsert(order, orderLines, ccXact);
				} catch (Exception e) {
					e.printStackTrace();
					client_error = true;

					break;
				}


//                O_ID++;
			}
			if (debug) {
				System.out.println("Thread finished: " + number_keys + " orders and xact inserted.");
			}

			barrier.countDown();
			client.closeClient();

		}

		public ResultHandler returnResults() {
			return partial_results;
		}
	}

}