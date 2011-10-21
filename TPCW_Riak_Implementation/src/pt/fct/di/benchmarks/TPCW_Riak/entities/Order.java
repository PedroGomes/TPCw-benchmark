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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pt.fct.di.benchmarks.TPCW_Riak.entities;

import org.uminho.gsd.benchmarks.interfaces.Entity;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;

public class Order implements Entity {

	public String O_ID;
	public String O_C_ID; // Customer customer?
	public String O_DATE;
	public double O_SUB_TOTAL;
	public Collection<OrderLine> orderLines;

	public Order() {
		orderLines = new ArrayList<OrderLine>();
	}

	public void addOrderLine(OrderLine ol) {
		if (orderLines == null)
			orderLines = new ArrayList<OrderLine>();
		orderLines.add(ol);
	}

	public OrderLine getOrderLine(String id) {
		for (OrderLine o : orderLines) {
			if (o.getOL_ID().equals(id))
				return o;
		}
		return null;
	}

	public String getO_DATE() {
		return O_DATE;
	}

	public void setO_DATE(String o_DATE) {
		O_DATE = o_DATE;
	}

	public double getO_SUB_TOTAL() {
		return O_SUB_TOTAL;
	}

	public void setO_SUB_TOTAL(double o_SUB_TOTAL) {
		O_SUB_TOTAL = o_SUB_TOTAL;
	}

	public double getO_TAX() {
		return O_TAX;
	}

	public void setO_TAX(double o_TAX) {
		O_TAX = o_TAX;
	}

	public double getO_TOTAL() {
		return O_TOTAL;
	}

	public void setO_TOTAL(double o_TOTAL) {
		O_TOTAL = o_TOTAL;
	}

	public String getO_SHIP_TYPE() {
		return O_SHIP_TYPE;
	}

	public void setO_SHIP_TYPE(String o_SHIP_TYPE) {
		O_SHIP_TYPE = o_SHIP_TYPE;
	}

	public String getO_SHIP_DATE() {
		return O_SHIP_DATE;
	}

	public void setO_SHIP_DATE(String o_SHIP_DATE) {
		O_SHIP_DATE = o_SHIP_DATE;
	}

	public String getO_STATUS() {
		return O_STATUS;
	}

	public void setO_STATUS(String o_STATUS) {
		O_STATUS = o_STATUS;
	}

	public String getO_BILL_ADDR_ID() {
		return O_BILL_ADDR_ID;
	}

	public void setO_BILL_ADDR_ID(String o_BILL_ADDR_ID) {
		O_BILL_ADDR_ID = o_BILL_ADDR_ID;
	}

	public String getO_SHIP_ADDR() {
		return O_SHIP_ADDR_ID;
	}

	public void setO_SHIP_ADDR(String o_SHIP_ADDR) {
		O_SHIP_ADDR_ID = o_SHIP_ADDR;
	}

	public void setO_ID(String o_ID) {
		O_ID = o_ID;
	}

	public void setO_C_ID(String o_C_ID) {
		O_C_ID = o_C_ID;
	}

	public double O_TAX;
	public double O_TOTAL;
	public String O_SHIP_TYPE;
	public String O_SHIP_DATE;
	public String O_STATUS;
	public String O_BILL_ADDR_ID;
	public String O_SHIP_ADDR_ID;

	public Order(String O_ID, String O_C_ID, String O_DATE, double O_SUB_TOTAL,
			double O_TAX, double O_TOTAL, String O_SHIP_TYPE, String shipDate,
			String O_STATUS, String billAddress, String O_SHIP_ADDR) {
		this.O_ID = O_ID;
		this.O_C_ID = O_C_ID;
		this.O_DATE = O_DATE;
		this.O_SUB_TOTAL = O_SUB_TOTAL;
		this.O_TAX = O_TAX;
		this.O_TOTAL = O_TOTAL;
		this.O_SHIP_TYPE = O_SHIP_TYPE;
		this.O_SHIP_DATE = shipDate;
		this.O_STATUS = O_STATUS;
		this.O_BILL_ADDR_ID = billAddress;
		this.O_SHIP_ADDR_ID = O_SHIP_ADDR;
	}

	public TreeMap<String, Object> getValuesToInsert() {

		TreeMap<String, Object> values = new TreeMap<String, Object>();

		values.put("O_C_ID", O_C_ID);
		values.put("O_DATE", O_DATE);
		values.put("O_SUB_TOTAL", O_SUB_TOTAL);
		values.put("O_TAX", O_TAX);
		values.put("O_TOTAL", O_TOTAL);

		values.put("O_SHIP_TYPE", O_SHIP_TYPE);
		values.put("O_SHIP_DATE", O_SHIP_DATE);

		values.put("O_BILL_ADDR_ID", O_BILL_ADDR_ID);
		values.put("O_SHIP_ADDR_ID", O_SHIP_ADDR_ID);

		values.put("O_STATUS", O_STATUS);

		values.put("O_ID", O_ID);
		
		values.put("orderLines", orderLines);

		return values;
	}

	public String getKeyName() {
		return "O_ID";
	}

	public String getO_ID() {
		return O_ID;
	}

	public String getO_C_ID() {
		return O_C_ID;
	}
}