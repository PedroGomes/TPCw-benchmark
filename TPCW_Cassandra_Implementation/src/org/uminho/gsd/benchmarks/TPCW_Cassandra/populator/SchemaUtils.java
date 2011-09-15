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
 * (c) 2011 Universidade do Minho. All rights reserved.
 * Written by Pedro Gomes and Nuno Carvalho.
 */

package org.uminho.gsd.benchmarks.TPCW_Cassandra.populator;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.thrift.transport.TTransportException;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class SchemaUtils {

	/**
	 * Generates the elements in fault within the schema, when running the application.
	 */
	public static void createSchema(String keyspace, List<Map<String,String>> column_families, List<Cassandra.Client> cassandraClients
	) throws Exception {


		// when starting the schema, check if the
		// keyspace is already defined.



		Cassandra.Client  cassandraClient = cassandraClients.get(0);


		for (Map<String, String> column_family : column_families) {
			String column_family_name = column_family.get("name");

			boolean containsColumn = false;

			List<CfDef> existing_column_families = null;

			existing_column_families = cassandraClient.describe_keyspace(keyspace).getCf_defs();

			for (CfDef extracted_column_family : existing_column_families) {
				if (extracted_column_family.name.equals(column_family_name)) {
					containsColumn = true;
				}
			}

			if (!containsColumn) {

				CfDef cfDef = new CfDef(keyspace, column_family_name);
				cfDef.setComparator_type(column_family.get("comparator"));
				if(column_family.containsKey("type")){
					cfDef.setColumn_type(column_family.get("type"));
				}
				if(column_family.containsKey("sub_comparator")){
					cfDef.setSubcomparator_type(column_family.get("sub_comparator"));
				}
				if(column_family.containsKey("read_repair_chance")){
					cfDef.setRead_repair_chance(Double.parseDouble(column_family.get("read_repair_chance")));
				}

				cassandraClient.system_add_column_family(cfDef);
				confirm_family_creation(cassandraClients,keyspace, column_family_name);

			}
		}
	}


	private static void confirm_family_creation(List<Cassandra.Client> node_connections,String keyspace, String column_family) throws Exception {


		for (Cassandra.Client node_connection : node_connections) {

			boolean found = false;

			while (!found) {
					KsDef keyspace_definition = node_connection.describe_keyspace(keyspace);
					for (CfDef cfDef : keyspace_definition.getCf_defs()) {
						if (column_family.equals(cfDef.getName())) {
							found = true;
						}
					}

				if (!found) {
					try {
						Thread.sleep(200);
					} catch (InterruptedException e) {
						//do nothing
					}
				}
			}
		}


	}


}
