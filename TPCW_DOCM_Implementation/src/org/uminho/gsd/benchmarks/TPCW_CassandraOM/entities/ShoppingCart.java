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

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;


@PersistenceCapable(detachable = "true")
public class ShoppingCart implements Entity {

    @PrimaryKey
    String i_id;
    @Persistent(defaultFetchGroup = "true")
    List<ShoppingCartLine> cart_lines;

    //Not used
    int SC_C_ID; //Unique identifier of the Shopping Session
    @Persistent
    Timestamp SC_DATE;//The date and time when the CART was last updated
    float SC_SUB_TOTAL; //The gross total amount of all items in the CART
    float SC_TAX; //The tax based on the gross total amount
    float SC_SHIP_COST; //The total shipping and handling charges
    float SC_TOTAL; //The total amount of the order
    String SC_C_FNAME; //C_FNAME of the Customer
    String SC_C_LNAME; //C_LNAME of the Customer
    float SC_C_DISCOUNT; //C_DISCOUNT of the Customer

    public ShoppingCart(String i_id) {
        cart_lines = new ArrayList<ShoppingCartLine>();
        this.i_id = i_id;
    }

    public String getI_id() {
        return i_id;
    }

    public void setI_id(String i_id) {
        this.i_id = i_id;
    }

    public TreeMap<String, Object> getValuesToInsert() {
        TreeMap<String, Object> values = new TreeMap<String, Object>();
        return values;
    }

    public List<ShoppingCartLine> getCart_lines() {
        return cart_lines;
    }

    public void addCartLine(ShoppingCartLine shoppingCartLine) {
        cart_lines.add(shoppingCartLine);
    }

    public String getKeyName() {
        return "SC_ID";
    }

    public Timestamp getSC_DATE() {
        return SC_DATE;
    }

    public void setSC_DATE(Timestamp SC_DATE) {
        this.SC_DATE = SC_DATE;
    }

    public int getSC_C_ID() {
        return SC_C_ID;
    }

    public void setSC_C_ID(int SC_C_ID) {
        this.SC_C_ID = SC_C_ID;
    }

    public float getSC_SUB_TOTAL() {
        return SC_SUB_TOTAL;
    }

    public void setSC_SUB_TOTAL(float SC_SUB_TOTAL) {
        this.SC_SUB_TOTAL = SC_SUB_TOTAL;
    }

    public float getSC_TAX() {
        return SC_TAX;
    }

    public void setSC_TAX(float SC_TAX) {
        this.SC_TAX = SC_TAX;
    }

    public float getSC_SHIP_COST() {
        return SC_SHIP_COST;
    }

    public void setSC_SHIP_COST(float SC_SHIP_COST) {
        this.SC_SHIP_COST = SC_SHIP_COST;
    }

    public float getSC_TOTAL() {
        return SC_TOTAL;
    }

    public void setSC_TOTAL(float SC_TOTAL) {
        this.SC_TOTAL = SC_TOTAL;
    }

    public String getSC_C_FNAME() {
        return SC_C_FNAME;
    }

    public void setSC_C_FNAME(String SC_C_FNAME) {
        this.SC_C_FNAME = SC_C_FNAME;
    }

    public String getSC_C_LNAME() {
        return SC_C_LNAME;
    }

    public void setSC_C_LNAME(String SC_C_LNAME) {
        this.SC_C_LNAME = SC_C_LNAME;
    }

    public float getSC_C_DISCOUNT() {
        return SC_C_DISCOUNT;
    }

    public void setSC_C_DISCOUNT(float SC_C_DISCOUNT) {
        this.SC_C_DISCOUNT = SC_C_DISCOUNT;
    }
}