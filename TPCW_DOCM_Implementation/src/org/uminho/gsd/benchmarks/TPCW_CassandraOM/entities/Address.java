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

@PersistenceCapable
public class Address implements Entity {

    @PrimaryKey
    String addr_id;
    String street1;
    String street2;
    String city;
    String state;
    String zip;
    @Persistent
    Country country;


    public Address(String key, String street1, String street2, String city, String state, String zip, Country country) {

        this.street1 = street1;
        this.street2 = street2;
        this.city = city;
        this.state = state;
        this.zip = zip;
        this.country = country;
        this.addr_id = key;
    }


    public String getAddr_id() {
        return addr_id;
    }

    public void setAddr_id(String addr_id) {
        this.addr_id = addr_id;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public Country getCountry() {
        return country;
    }

    public void setCountry(Country country) {
        this.country = country;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getStreet1() {
        return street1;
    }

    public void setStreet1(String street1) {
        this.street1 = street1;
    }

    public String getStreet2() {
        return street2;
    }

    public void setStreet2(String street2) {
        this.street2 = street2;
    }

    public String getZip() {
        return zip;
    }

    public void setZip(String zip) {
        this.zip = zip;
    }

    public TreeMap<String, Object> getValuesToInsert() {
        TreeMap<String, Object> values = new TreeMap<String, Object>();

        values.put("ADDR_STREET1", street1);
        values.put("ADDR_STREET2", street2);
        values.put("ADDR_CITY", city);
        values.put("ADDR_STATE", state);
        values.put("ADDR_ZIP", zip);
        values.put("ADDR_CO_ID", country);


        return values;
    }

    public String getKeyName() {
        return "ADDR_ID";
    }


    @Override
    public String toString() {
        return "Address{" +
                "addr_id='" + addr_id + '\'' +
                ", street1='" + street1 + '\'' +
                ", street2='" + street2 + '\'' +
                ", city='" + city + '\'' +
                ", state='" + state + '\'' +
                ", zip='" + zip + '\'' +
                ", country=" + country +
                '}';
    }
}