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
import org.uminho.gsd.benchmarks.benchmark.BenchmarkExecutor;
import org.uminho.gsd.benchmarks.helpers.TPM_counter;
import org.uminho.gsd.benchmarks.interfaces.executor.AbstractDatabaseExecutorFactory;
import org.uminho.gsd.benchmarks.interfaces.executor.DatabaseExecutorInterface;

import java.util.Map;
import java.util.TreeMap;

public class TPCW_MySQL_Factory extends AbstractDatabaseExecutorFactory {


    Logger log = Logger.getLogger(TPCW_MySQL_Factory.class);

    private String database = "";
    private String user = "";
    private String password = "";

    Map<String, String> readPaths;
    Map<String, String> writesPaths;


    public TPCW_MySQL_Factory(BenchmarkExecutor executor, String conf_file) {
        super(executor, conf_file);
        init();
    }

    private void init() {

        readPaths = new TreeMap<String, String>();
        writesPaths = new TreeMap<String, String>();

        Map<String, String> masters;
        Map<String, String> slaves = null;


		if (conf.containsKey("Isolation_level")) {
            database = conf.get("Isolation_level").get("level");

        }



        if (conf.containsKey("DataBaseInfo")) {
            database = conf.get("DataBaseInfo").get("database");
            user = conf.get("DataBaseInfo").get("user");
            password = conf.get("DataBaseInfo").get("password");

        } else {
            log.error("[MYSQL FACTORY]: No information regarding the used database and the related user");
        }

        //Masters
        if (conf.containsKey("MasterConnections")) {
            masters = conf.get("MasterConnections");
        } else {
            log.error("[MYSQL FACTORY]: No information regarding the master nodes");
            return;
        }

        //Slaves
        if (conf.containsKey("SlaveConnections")) {
            slaves = conf.get("SlaveConnections");
        } else {
            log.error("[MYSQL FACTORY]: No information regarding the slave nodes");
        }

        //Read Write Paths
        if (conf.containsKey("ReadWritePaths")) {

            String write_options = conf.get("ReadWritePaths").get("Writes");
            String read_options = conf.get("ReadWritePaths").get("Reads");

            //masters
            for (Map.Entry master_info : masters.entrySet()) {
                if (write_options.equalsIgnoreCase("Master") || write_options.equalsIgnoreCase("All")) {
                    writesPaths.put((String) master_info.getKey(), (String) master_info.getValue());
                }
                if (read_options.equalsIgnoreCase("Master") || read_options.equalsIgnoreCase("All")) {
                    readPaths.put((String) master_info.getKey(), (String) master_info.getValue());
                }

            }

            //slaves
            if (slaves != null) {
                for (Map.Entry slave_info : slaves.entrySet()) {
                    if (write_options.equalsIgnoreCase("Slaves") || write_options.equalsIgnoreCase("Slave") || write_options.equalsIgnoreCase("All")) {
                        writesPaths.put((String) slave_info.getKey(), (String) slave_info.getValue());
                    }
                    if (read_options.equalsIgnoreCase("Slaves") || read_options.equalsIgnoreCase("Slave") || read_options.equalsIgnoreCase("All")) {
                        readPaths.put((String) slave_info.getKey(), (String) slave_info.getValue());
                    }
                }

            }

        } else {
            log.error("[MYSQL FACTORY]: No information regarding the write/read paths");
        }


		initTPMCounting();

    }




    @Override
    public DatabaseExecutorInterface getDatabaseClient() {


		TPM_counter tpm_counter = new TPM_counter();
		registerCounter(tpm_counter);
        return new TPCW_MySQL_Executor(database, user ,password, readPaths, writesPaths,this.client_number,tpm_counter);

    }
}
