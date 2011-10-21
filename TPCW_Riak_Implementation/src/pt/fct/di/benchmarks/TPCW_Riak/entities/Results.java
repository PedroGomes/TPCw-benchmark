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
package pt.fct.di.benchmarks.TPCW_Riak.entities;

import org.uminho.gsd.benchmarks.interfaces.Entity;

import java.util.TreeMap;

public class Results implements Entity {

	public  int BOUGHT;
	public int STOCK;
	public  String CLIENT_ID;

    public Results(int bought, int totalStock, String clientID) {
        BOUGHT = bought;
        STOCK = totalStock;
        CLIENT_ID = clientID;
    }

    public String getKeyName() {
        return "ITEM_ID";
    }

    public TreeMap<String, Object> getValuesToInsert() {
        TreeMap<String, Object> values = new TreeMap<String, Object>();

        values.put("BOUGHT", BOUGHT);
        values.put("STOCK", STOCK);
        values.put("CLIENT_ID", CLIENT_ID);


        return values;
    }
}