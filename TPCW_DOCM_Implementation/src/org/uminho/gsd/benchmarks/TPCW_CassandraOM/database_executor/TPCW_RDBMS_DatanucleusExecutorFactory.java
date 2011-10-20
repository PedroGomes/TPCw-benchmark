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

package org.uminho.gsd.benchmarks.TPCW_CassandraOM.database_executor;


import org.uminho.gsd.benchmarks.benchmark.BenchmarkExecutor;
import org.uminho.gsd.benchmarks.generic.helpers.NodeKeyGenerator;
import org.uminho.gsd.benchmarks.helpers.TPM_counter;
import org.uminho.gsd.benchmarks.interfaces.KeyGenerator;
import org.uminho.gsd.benchmarks.interfaces.executor.AbstractDatabaseExecutorFactory;
import org.uminho.gsd.benchmarks.interfaces.executor.DatabaseExecutorInterface;


/**
 * TPC-W execution factory  interface for Cassandra
 * It loads the configuration for Cassandra and returns execution clients.
 */
public class TPCW_RDBMS_DatanucleusExecutorFactory extends AbstractDatabaseExecutorFactory {


    KeyGenerator keyGenerator;

    public TPCW_RDBMS_DatanucleusExecutorFactory(BenchmarkExecutor executor, String conf_file) {
        super(executor, conf_file);
        init();
    }

    private void init() {

		initTPMCounting();

    }

    @Override
    public DatabaseExecutorInterface getDatabaseClient() {

        if (keyGenerator == null && nodeID != null) {
            keyGenerator = new NodeKeyGenerator(this.nodeID.getId());
        }

		TPM_counter tpm_counter = new TPM_counter();
		registerCounter(tpm_counter);

        return new TPCW_RDBMS_DataNucleusExecutor(keyGenerator,tpm_counter);

    }
}