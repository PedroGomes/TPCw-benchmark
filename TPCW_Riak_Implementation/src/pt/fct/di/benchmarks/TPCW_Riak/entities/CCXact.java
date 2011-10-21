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

import java.sql.Date;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;


public class CCXact implements Entity {


	Collection<CCXactItem> items; 
	
	public CCXact(){
		items = new ArrayList<CCXactItem>();
	}
	
    public CCXact(String type, long num, String name, String expiry, double total, String shipDate, String order, int country) {
    	items = new ArrayList<CCXactItem>();
		CCXactItem item = new CCXactItem(type,num,name,expiry,total,shipDate,order,country);
		items.add(item);
    }

    public TreeMap<String, Object> getValuesToInsert() {
        TreeMap<String, Object> values = new TreeMap<String, Object>();

        System.out
				.println("Analisar o que fazer caso este metodo seja necessario");
//        values.put("CX_TYPE", CX_TYPE);
//        values.put("CX_CC_NUM", CX_CC_NUM);
//        values.put("CX_CC_NAME", CX_CC_NAME);
//        values.put("CX_CC_EXPIRY", CX_CC_EXPIRY);
//        values.put("CX_XACT_DATE", CX_XACT_DATE);
//        values.put("CX_XACT_AMT", CX_XACT_AMT);
//        values.put("CX_CO_ID", CX_CO_ID);

        return values;
    }
    
    public CCXactItem getItem(String order_id){
    	for(CCXactItem item :items){
    		if(item.getCX_O_ID().equals(order_id))
    			return item;
    	}
    	return null;
    }
    
    public void putItem(CCXactItem item){
    	items.add(item);
    }

    public String getKeyName() {
        return "CX_O_ID";
    }


}