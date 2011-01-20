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
import java.util.TreeMap;


@PersistenceCapable(detachable = "true")
public class ShoppingCartLine implements Entity {

    @PrimaryKey
    String ShoppingCartLineID;

    String ShoppingCartID;
    @Persistent(defaultFetchGroup = "true")
    Item book;
    int qty;


    float SCL_COST;// The cost of the item in the CART
    float SCL_SRP;// The list price for the item in the CART
    String SCL_TITLE;// The title of the item in the CART
    String SCL_BACKING;// The backing of the item in the CART

    public ShoppingCartLine(String ShoppingCartID, Item book, int qty) {

        this.ShoppingCartLineID = ShoppingCartID + book.getI_id();
        this.ShoppingCartID = ShoppingCartID;
        this.book = book;


        this.qty = qty;
    }

    public String getShoppingCartID() {
        return ShoppingCartID;
    }

    public Item getBook() {
        return book;
    }

    public String getShoppingCartLineID() {
        return ShoppingCartLineID;
    }

    public void setShoppingCartLineID(String shoppingCartLineID) {
        ShoppingCartLineID = shoppingCartLineID;
    }

    public void setBook(Item book) {
        this.book = book;

    }

    public int getQty() {
        return qty;
    }

    public void setQty(int qty) {
        this.qty = qty;
    }

    public float getSCL_COST() {
        return SCL_COST;
    }

    public void setSCL_COST(float SCL_COST) {
        this.SCL_COST = SCL_COST;
    }

    public float getSCL_SRP() {
        return SCL_SRP;
    }

    public void setSCL_SRP(float SCL_SRP) {
        this.SCL_SRP = SCL_SRP;
    }

    public String getSCL_TITLE() {
        return SCL_TITLE;
    }

    public void setSCL_TITLE(String SCL_TITLE) {
        this.SCL_TITLE = SCL_TITLE;
    }

    public String getSCL_BACKING() {
        return SCL_BACKING;
    }

    public void setSCL_BACKING(String SCL_BACKING) {
        this.SCL_BACKING = SCL_BACKING;
    }

    public TreeMap<String, Object> getValuesToInsert() {
        TreeMap<String, Object> values = new TreeMap<String, Object>();

        values.put("QTY", qty);
        values.put("KEY_BOOK", book.getI_id());

        return values;
    }

    public String getKeyName() {
        return "KEY_SHOPPING_CART";
    }


}