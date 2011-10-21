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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;

public class ShoppingCart implements Entity {

	public String i_id;

	public String SC_C_ID; // Unique identifier of the Shopping Session
	// public String SC_DATE;// The date and time when the CART was last updated
	// public float SC_SUB_TOTAL; // The gross total amount of all items in the
	// CART
	// public float SC_TAX; // The tax based on the gross total amount
	// public float SC_SHIP_COST; // The total shipping and handling charges
	// public float SC_TOTAL; // The total amount of the order
	public String SC_C_FNAME; // C_FNAME of the Customer
	public String SC_C_LNAME; // C_LNAME of the Customer
	public float SC_C_DISCOUNT; // C_DISCOUNT of the Customer
	public CartInfo cartInfo;
	public Collection<SCLine> SCLine;

	// TODO Estou a assumir que i_id e SC_C_ID sao a mesma coisa por isso estou
	// a modificar as duas
	public ShoppingCart(String i_id) {
		this.i_id = i_id;
		this.SC_C_ID = i_id;
		SCLine = new ArrayList<SCLine>();
	}

	public String getI_id() {
		return i_id;
	}

	public void setI_id(String i_id) {
		this.i_id = i_id;
		this.SC_C_ID = i_id;
	}

	public TreeMap<String, Object> getValuesToInsert() {
		TreeMap<String, Object> values = new TreeMap<String, Object>();

		values.put("SC_C_ID", SC_C_ID);
		values.put("SC_C_FNAME", SC_C_FNAME);
		values.put("SC_C_LNAME", SC_C_LNAME);
		values.put("SC_C_DISCOUNT", SC_C_DISCOUNT);
		values.put("SCLine",SCLine);

		return values;
	}

	public String getKeyName() {
		return "SC_C_ID";
	}

	public void setCartInfo(String attribute, Object value) {
		if (cartInfo == null) {
			cartInfo = new CartInfo();
		}
		if (attribute.equals("SC_DATE"))
			cartInfo.setSC_DATE((String) value);
		if (attribute.equals("SC_SUB_TOTAL"))
			cartInfo.setSC_SUB_TOTAL((Float) value);
		if (attribute.equals("SC_TAX"))
			cartInfo.setSC_TAX((Float) value);
		if (attribute.equals("SC_TOTAL"))
			cartInfo.setSC_TOTAL((Float) value);
		if (attribute.equals("SC_SHIP_COST"))
			cartInfo.setSC_SHIP_COST((Float) value);

	}

	public Object getCartInfo(String attribute) {
		if (cartInfo == null) {
			return null;
		}
		if (attribute.equals("SC_DATE"))
			return cartInfo.getSC_DATE();
		if (attribute.equals("SC_SUB_TOTAL"))
			return cartInfo.getSC_SUB_TOTAL();
		if (attribute.equals("SC_TAX"))
			return cartInfo.getSC_TAX();
		if (attribute.equals("SC_TOTAL"))
			return cartInfo.getSC_TOTAL();
		if (attribute.equals("SC_SHIP_COST"))
			return cartInfo.getSC_SHIP_COST();

		return null;

	}
	
	public void addSCLine(SCLine line) {
		SCLine.add(line);
	}

	public SCLine getSCLine(int scl_i) {
		for (SCLine sc : SCLine) {
			if (sc.I_ID == scl_i)
				return sc;
		}
		return null;
	}
	
	public Collection<SCLine> getSCLines() {
		return SCLine;
	}


}