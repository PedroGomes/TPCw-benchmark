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
import java.util.TreeMap;


@PersistenceCapable(detachable = "true")
@Index(table = "author_index",members={"A_LNAME"})
public class Author implements Entity {

    String fname;
   // @Index
    String A_LNAME;
    String mname;
    @Persistent
    Date dob;
    String bio;

    @PrimaryKey
    int a_id;


    public Author(int a_id, String fname, String lname, String mname, Date dob, String bio) {
        this.fname = fname;
        this.A_LNAME = lname;
        this.mname = mname;
        this.dob = dob;
        this.bio = bio;
        this.a_id = a_id;
    }

    public int getA_id() {
        return a_id;
    }

    public String getBio() {
        return bio;
    }

    public void setBio(String bio) {
        this.bio = bio;
    }

    public Date getDob() {
        return dob;
    }

    public void setDob(Date dob) {
        this.dob = dob;
    }

    public String getFname() {
        return fname;
    }

    public void setFname(String fname) {
        this.fname = fname;
    }

    public String getA_LNAME() {
        return A_LNAME;
    }

    public void setA_LNAME(String a_LNAME) {
        this.A_LNAME = a_LNAME;
    }

    public String getMname() {
        return mname;
    }

    public void setMname(String mname) {
        this.mname = mname;
    }

    public TreeMap<String, Object> getValuesToInsert() {
        TreeMap<String, Object> values = new TreeMap<String, Object>();

        values.put("A_FNAME", fname);
        values.put("A_LNAME", A_LNAME);
        values.put("A_MNAME", mname);
        values.put("A_DOB", dob);
        values.put("A_BIO", bio);

        return values;
    }

    public String getKeyName() {
        return "A_ID";
    }

}