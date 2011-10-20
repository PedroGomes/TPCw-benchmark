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
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.uminho.gsd.benchmarks.TPCW_CassandraOM.entities;

import org.uminho.gsd.benchmarks.interfaces.Entity;

import javax.jdo.annotations.Index;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;
import java.sql.Date;
import java.util.List;
import java.util.TreeMap;

/**
 * O_ID
 * O_C_ID
 * O_DATE
 * O_SUB_TOTAL
 * O_TAX
 * O_TOTAL
 * O_SHIP_TYPE
 * O_SHIP_DATE
 * O_BILL_ADDR_ID
 * O_SHIP_ADDR_ID
 * O_STATUS
 */

@PersistenceCapable
@Index(table="order_index",members={"O_C_ID"})
public class Order implements Entity {

    @PrimaryKey
    String O_ID;
    //@Index
    @Persistent
    Customer O_C;
	String O_C_ID;
    @Persistent
    Date O_DATE;
    float O_SUB_TOTAL;
    float O_TAX;
    float O_TOTAL;
    String O_SHIP_TYPE;
    @Persistent
    Date O_SHIP_DATE;
    String O_STATUS;
    String billAddress;
    String O_SHIP_ADDR;
    @Persistent(defaultFetchGroup = "true")
    List<OrderLine> orderlines;




    public Order(String O_ID, Customer O_C, Date O_DATE, float O_SUB_TOTAL, float O_TAX, float O_TOTAL, String O_SHIP_TYPE, Date shipDate, String O_STATUS, String billAddress, String O_SHIP_ADDR) {
        this.O_ID = O_ID;
        this.O_C = O_C;
        this.O_DATE = O_DATE;
        this.O_SUB_TOTAL = O_SUB_TOTAL;
        this.O_TAX = O_TAX;
        this.O_TOTAL = O_TOTAL;
        this.O_SHIP_TYPE = O_SHIP_TYPE;
        this.O_SHIP_DATE = shipDate;
        this.O_STATUS = O_STATUS;
        this.billAddress = billAddress;
        this.O_SHIP_ADDR = O_SHIP_ADDR;
		if(O_C!=null)
			this.O_C_ID = O_C.getC_id();
    }

    public String getO_ID() {
        return O_ID;
    }

    public void setO_ID(String o_ID) {
        O_ID = o_ID;
    }

    public Customer getO_C() {
        return O_C;
    }

    public void setO_C(Customer O_C_ID) {
        this.O_C = O_C_ID;
		this.O_C_ID = O_C_ID.getC_id();
    }

    public String getO_SHIP_ADDR() {
        return O_SHIP_ADDR;
    }

    public void setO_SHIP_ADDR(String O_SHIP_ADDR) {
        this.O_SHIP_ADDR = O_SHIP_ADDR;
    }

    public String getO_SHIP_TYPE() {
        return O_SHIP_TYPE;
    }

    public void setO_SHIP_TYPE(String O_SHIP_TYPE) {
        this.O_SHIP_TYPE = O_SHIP_TYPE;
    }

    public String getO_STATUS() {
        return O_STATUS;
    }

    public void setO_STATUS(String O_STATUS) {
        this.O_STATUS = O_STATUS;
    }

    public float getO_SUB_TOTAL() {
        return O_SUB_TOTAL;
    }

    public void setO_SUB_TOTAL(float O_SUB_TOTAL) {
        this.O_SUB_TOTAL = O_SUB_TOTAL;
    }

    public float getO_TAX() {
        return O_TAX;
    }

    public void setO_TAX(float O_TAX) {
        this.O_TAX = O_TAX;
    }

    public float getO_TOTAL() {
        return O_TOTAL;
    }

    public void setO_TOTAL(float O_TOTAL) {
        this.O_TOTAL = O_TOTAL;
    }

    public String getBillAddress() {
        return billAddress;
    }

    public void setBillAddress(String billAddress) {
        this.billAddress = billAddress;
    }

    public Date getO_DATE() {
        return O_DATE;
    }

    public void setO_DATE(Date o_DATE) {
        O_DATE = o_DATE;
    }

    public Date getO_SHIP_DATE() {
        return O_SHIP_DATE;
    }

    public void setO_SHIP_DATE(Date o_SHIP_DATE) {
        O_SHIP_DATE = o_SHIP_DATE;
    }

    public List<OrderLine> getOrderlines() {
        return orderlines;
    }

    public void setOrderlines(List<OrderLine> orderlines) {
        this.orderlines = orderlines;
    }

    public TreeMap<String, Object> getValuesToInsert() {

//        O_ID
//O_C_ID
//O_DATE
//O_SUB_TOTAL
//O_TAX
//O_TOTAL
//O_SHIP_TYPE
//O_SHIP_DATE
//O_BILL_ADDR_ID
//O_SHIP_ADDR_ID
//O_STATUS
//

        TreeMap<String, Object> values = new TreeMap<String, Object>();

        values.put("O_C_ID", O_C);
        values.put("O_DATE", O_DATE);
        values.put("O_SUB_TOTAL", O_SUB_TOTAL);
        values.put("O_TAX", O_TAX);
        values.put("O_TOTAL", O_TOTAL);

        values.put("O_SHIP_TYPE", O_SHIP_TYPE);
        values.put("O_SHIP_DATE", O_SHIP_DATE);

        values.put("O_BILL_ADDR_ID", billAddress);
        values.put("O_SHIP_ADDR_ID", O_SHIP_ADDR);

        values.put("O_STATUS", O_STATUS);


        return values;
    }

    public String getKeyName() {
        return "O_ID";
    }
}