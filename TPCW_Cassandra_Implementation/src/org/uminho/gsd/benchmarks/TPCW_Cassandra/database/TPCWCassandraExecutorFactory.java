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


import org.apache.cassandra.thrift.ConsistencyLevel;
import org.uminho.gsd.benchmarks.generic.helpers.NodeKeyGenerator;
import org.uminho.gsd.benchmarks.benchmark.BenchmarkExecutor;
import org.uminho.gsd.benchmarks.helpers.TPM_counter;
import org.uminho.gsd.benchmarks.helpers.ThinkTime;
import org.uminho.gsd.benchmarks.interfaces.executor.AbstractDatabaseExecutorFactory;
import org.uminho.gsd.benchmarks.interfaces.executor.DatabaseExecutorInterface;

import java.util.Map;
import java.util.TreeMap;


/**
 * TPC-W execution factory  interface for Cassandra
 * It loads the configuration for Cassandra and returns execution clients.
 */
public class TPCWCassandraExecutorFactory extends AbstractDatabaseExecutorFactory {

    //Consistency level mapping
    private TreeMap<String, ConsistencyLevel> consistencyMapping;
    /**
     * KeySpace name*
     */
    protected String Keyspace;
    //Database nodes connection info
    private Map<String, Integer> connections = new TreeMap<String, Integer>();

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
     * Consistency level array to pass to client*
     */
    private ConsistencyLevel[] consistencyLevel;
    /**
     * Think time*
     */
    private int simulatedDelay;
    /**
     * The number of keys to fetch from the database in each iteration*
     */
    private int search_slice_ratio;


    NodeKeyGenerator keyGenerator;



    private Map<String, String> key_associations;

    public TPCWCassandraExecutorFactory(BenchmarkExecutor executor, String conf_file) {
        super(executor, conf_file);
        init();
    }


    private void init() {

        consistencyMapping = new TreeMap<String, ConsistencyLevel>();

        consistencyMapping.put("ZERO", ConsistencyLevel.ANY);
        consistencyMapping.put("ONE", ConsistencyLevel.ONE);
        consistencyMapping.put("QUORUM", ConsistencyLevel.QUORUM);
        consistencyMapping.put("ALL", ConsistencyLevel.ALL);

        if (!conf.containsKey("ConsistencyLevels")) {
            System.out.println("WARNING: CONSISTENCY LEVELS NOT FOUND, QUORUM ASSUMED");
        } else {
            Map<String, String> CL = (Map<String, String>) conf.get("ConsistencyLevels");
            consistencyLevel = new ConsistencyLevel[6];

            INSERT_CONSISTENCY_LEVEL = consistencyMapping.get(CL.get("INSERT_CONSISTENCY_LEVEL"));
            consistencyLevel[0] = INSERT_CONSISTENCY_LEVEL;
            REMOVE_CONSISTENCY_LEVEL = consistencyMapping.get(CL.get("REMOVE_CONSISTENCY_LEVEL"));
            consistencyLevel[1] = REMOVE_CONSISTENCY_LEVEL;
            RANGE_CONSISTENCY_LEVEL = consistencyMapping.get(CL.get("RANGE_CONSISTENCY_LEVEL"));
            consistencyLevel[2] = RANGE_CONSISTENCY_LEVEL;
            TRANSACTIONAL_CONSISTENCY_LEVEL = consistencyMapping.get(CL.get("TRANSACTIONAL_CONSISTENCY_LEVEL"));
            consistencyLevel[3] = TRANSACTIONAL_CONSISTENCY_LEVEL;
            READ_CONSISTENCY_LEVEL = consistencyMapping.get(CL.get("READ_CONSISTENCY_LEVEL"));
            consistencyLevel[4] = READ_CONSISTENCY_LEVEL;
            WRITE_CONSISTENCY_LEVEL = consistencyMapping.get(CL.get("WRITE_CONSISTENCY_LEVEL"));
            consistencyLevel[5] = WRITE_CONSISTENCY_LEVEL;
        }


        Keyspace = "Tpcw";

        if (!conf.containsKey("DataBaseInfo")) {
            System.out.println("ERROR: NO DATABASE INFO FOUND DEFAULTS ASSUMED: KEYSPACE=Tpcw");
        } else {
            Map<String, String> CI = (Map<String, String>) conf.get("DataBaseInfo");
            Keyspace = CI.get("keyspace");
        }


        if (!conf.containsKey("DataBaseConnections")) {
            System.out.println("ERROR: NO CONNECTION INFO FOUND DEFAULTS ASSUMED: [HOST=localhost, PORT=9160] ");
            connections.put("localhost", 9160);
        } else {
            Map<String, String> CI = (Map<String, String>) conf.get("DataBaseConnections");
            for (String host : CI.keySet()) {
                int port = Integer.parseInt(CI.get(host).trim());
                connections.put(host, port);
                System.out.println("Cassandra native database client registered: "+host + ":" +port);
            }
        }
        if (connections.isEmpty()) {
            System.out.println("ERROR: NO CONNECTION INFO FOUND DEFAULTS ASSUMED: [HOST=localhost, PORT=9160] ");
            connections.put("localhost", 9160);
        }

        if (!conf.containsKey("ColumnPaths")) {
            System.out.println("WARNING: KEY ASSOCIATIONS NOT FOUND");
            key_associations = new TreeMap<String, String>();
        } else {
            key_associations = conf.get("ColumnPaths");
        }

        if (!conf.containsKey("Configuration")) {
            System.out.println("[WARN:] RETRIEVED SLICES -> 1000 rows, add \"Configuration\" section and a \"retrievedRowSlices\" parameter");
            search_slice_ratio = 1000;
        } else {
            Map<String, String> CI = conf.get("Configuration");

            if (CI.containsKey("retrievedRowSlices")) {
                search_slice_ratio = Integer.parseInt(CI.get("retrievedRowSlices"));
            } else {
                System.out.println("[ERROR:] NO CONFIGURATION FOUND: RETRIEVED SLICES -> 1000 rows, add a \"retrievedRowSlices\" parameter to the \"Configuration\" section");
                search_slice_ratio = 1000;


            }
        }
        if (connections.isEmpty()) {
            System.out.println("ERROR: NO CONNECTION INFO FOUND DEFAULTS ASSUMED: [HOST=localhost, PORT=9160] ");
            connections.put("localhost", 9160);
        }

        System.out.println("Think Time Sample: "+ ThinkTime.getThinkTime()+","+ThinkTime.getThinkTime());

		initTPMCounting();

    }


    @Override
    public DatabaseExecutorInterface getDatabaseClient() {

        if (keyGenerator == null && nodeID != null) {
            keyGenerator = new NodeKeyGenerator(this.nodeID.getId());
        }

		TPM_counter tpm_counter = new TPM_counter();
		registerCounter(tpm_counter);

        return new org.uminho.gsd.benchmarks.TPCW_Cassandra.database.TPCWCassandraExecutor(Keyspace, connections, consistencyLevel, key_associations, simulatedDelay, search_slice_ratio, keyGenerator,tpm_counter);

    }
}