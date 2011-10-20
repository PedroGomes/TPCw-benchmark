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

import javax.jdo.annotations.*;
import java.sql.Date;
import java.util.List;
import java.util.TreeMap;


/**
 * I_ID
 * I_TITLE
 * I_A_ID
 * I_PUB_DATE X
 * I_PUBLISHER
 * I_SUBJECT
 * I_DESC
 * I_RELATED[1 -5]
 * I_THUMBNAIL X
 * I_IMAGE X
 * I_SRP X
 * I_COST
 * I_AVAIL
 * I_STOCK
 * I_ISBN
 * I_PAGE
 * I_BACKING
 * I_DIMENSION
 */
@PersistenceCapable(detachable = "true")
@Indices({@Index(name="subject_index",table = "item_subject_index",members={"I_SUBJECT"}), @Index(name="title_index",table = "item_title_index", members={"I_TITLE"}),@Index(name="author_index",table = "item_author_index", members={"I_AUTHOR"})})
public class Item implements Entity {

    @PrimaryKey
    int i_id;
   // @Index
    String I_TITLE;
    @Persistent
    Date pubDate;
   // @Index
    @Persistent(defaultFetchGroup = "true")
    Author I_AUTHOR;
	int I_AUTHOR_ID;
    String I_PUBLISHER;
    String I_DESC;
   // @Index
    String I_SUBJECT;
    String thumbnail;
    String image;
    float I_COST;
    long I_STOCK;
    String isbn;//international id
    double srp;//Suggested Retail Price
    int I_RELATED1;
    int I_RELATED2;
    int I_RELATED3;
    int I_RELATED4;
    int I_RELATED5;
    int I_PAGE;
    @Persistent
    Date avail; //Data when available
    String I_BACKING;
    String dimensions;


    public Item(int i_id, String I_TITLE, Date pubDate, String I_PUBLISHER, String I_DESC, String I_SUBJECT, String thumbnail, String image, float I_COST, long I_STOCK, String isbn, double srp, List<Integer> I_RELATED, int I_PAGE, Date avail, String I_BACKING, String dimensions, Author author) {
        this.i_id = i_id;
        this.I_TITLE = I_TITLE;
        this.pubDate = pubDate;
        this.I_AUTHOR = author;
        this.I_PUBLISHER = I_PUBLISHER;
        this.I_DESC = I_DESC;
        this.I_SUBJECT = I_SUBJECT;
        this.thumbnail = thumbnail;
        this.image = image;
        this.I_COST = I_COST;
        this.I_STOCK = I_STOCK;
        this.isbn = isbn;
        this.srp = srp;
        this.I_RELATED1 = I_RELATED.get(0);
        this.I_RELATED2 = I_RELATED.get(1);
        this.I_RELATED3 = I_RELATED.get(2);
        this.I_RELATED4 = I_RELATED.get(3);
        this.I_RELATED5 = I_RELATED.get(4);
        this.I_PAGE = I_PAGE;
        this.avail = avail;
        this.I_BACKING = I_BACKING;
        this.dimensions = dimensions;
		if(author!=null){
			I_AUTHOR_ID = author.a_id;
		}
    }

    public Author getI_AUTHOR() {
        return I_AUTHOR;

    }

    public void setI_AUTHOR(Author I_AUTHOR) {
        this.I_AUTHOR = I_AUTHOR;
		if(I_AUTHOR!=null){
			I_AUTHOR_ID = I_AUTHOR.a_id;
		}
    }

    public String getI_BACKING() {
        return I_BACKING;
    }

    public void setI_BACKING(String I_BACKING) {
        this.I_BACKING = I_BACKING;
    }

    public int getI_id() {
        return i_id;
    }

    public float getI_COST() {

        return I_COST;
    }

    public void setI_COST(float I_COST) {
        this.I_COST = I_COST;
    }

    public String getI_DESC() {
        return I_DESC;
    }

    public void setI_DESC(String I_DESC) {
        this.I_DESC = I_DESC;
    }

    public int getI_PAGE() {
        return I_PAGE;
    }

    public void setI_PAGE(int I_PAGE) {
        this.I_PAGE = I_PAGE;
    }

    public String getI_PUBLISHER() {
        return I_PUBLISHER;
    }

    public void setI_PUBLISHER(String I_PUBLISHER) {
        this.I_PUBLISHER = I_PUBLISHER;
    }


    public long getI_STOCK() {
        return I_STOCK;
    }

    public void setI_STOCK(long I_STOCK) {
        this.I_STOCK = I_STOCK;
    }

    public String getI_SUBJECT() {
        return I_SUBJECT;
    }

    public void setI_SUBJECT(String I_SUBJECT) {
        this.I_SUBJECT = I_SUBJECT;
    }

    public String getI_TITLE() {
        return I_TITLE;
    }

    public void setI_TITLE(String I_TITLE) {
        this.I_TITLE = I_TITLE;
    }


    public Date getAvail() {
        return avail;
    }

    public void setAvail(Date avail) {
        this.avail = avail;
    }

    public String getDimensions() {
        return dimensions;
    }

    public void setDimensions(String dimensions) {
        this.dimensions = dimensions;
    }

    public int getI_RELATED1() {
        return I_RELATED1;
    }

    public int getI_RELATED3() {
        return I_RELATED3;
    }

    public int getI_RELATED2() {
        return I_RELATED2;
    }

    public int getI_RELATED4() {
        return I_RELATED4;
    }

    public int getI_RELATED5() {
        return I_RELATED5;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getIsbn() {
        return isbn;
    }

    public void setIsbn(String isbn) {
        this.isbn = isbn;
    }

    public Date getPubDate() {
        return pubDate;
    }

    public void setPubDate(Date pubDate) {
        this.pubDate = pubDate;
    }

    public double getSrp() {
        return srp;
    }

    public void setSrp(double srp) {
        this.srp = srp;
    }

    public String getThumbnail() {
        return thumbnail;
    }

    public void setThumbnail(String thumbnail) {
        this.thumbnail = thumbnail;
    }

    public void setI_RELATED1(int i_RELATED1) {
        I_RELATED1 = i_RELATED1;
    }

    public void setI_RELATED2(int i_RELATED2) {
        I_RELATED2 = i_RELATED2;
    }

    public void setI_RELATED3(int i_RELATED3) {
        I_RELATED3 = i_RELATED3;
    }

    public void setI_RELATED4(int i_RELATED4) {
        I_RELATED4 = i_RELATED4;
    }

    public void setI_RELATED5(int i_RELATED5) {
        I_RELATED5 = i_RELATED5;
    }

    public TreeMap<String, Object> getValuesToInsert() {
//
//
//        I_ID
//I_TITLE
//I_A_ID
//I_PUB_DATE
//I_PUBLISHER
//I_SUBJECT
//I_DESC
//I_RELATED[1-5]
//I_THUMBNAIL
//I_IMAGE
//I_SRP
//I_COST
//I_AVAIL
//I_STOCK
//I_ISBN
//I_PAGE
//I_BACKING
//I_DIMENSION


        TreeMap<String, Object> values = new TreeMap<String, Object>();

        values.put("I_TITLE", I_TITLE);
        values.put("I_A_ID", I_AUTHOR);
        values.put("I_PUB_DATE", pubDate);
        values.put("I_PUBLISHER", I_PUBLISHER);
        values.put("I_SUBJECT", I_SUBJECT);
        values.put("I_DESC", I_DESC);
        values.put("I_RELATED1", I_RELATED1);
        values.put("I_RELATED2", I_RELATED2);
        values.put("I_RELATED3", I_RELATED3);
        values.put("I_RELATED4", I_RELATED4);
        values.put("I_RELATED5", I_RELATED5);
        values.put("I_THUMBNAIL", thumbnail);
        values.put("I_IMAGE", image);
        values.put("I_SRP", srp);
        values.put("I_COST", I_COST);
        values.put("I_AVAIL", avail);
        values.put("I_STOCK", I_STOCK);
        values.put("I_ISBN", isbn);
        values.put("I_PAGE", I_PAGE);
        values.put("I_BACKING", I_BACKING);
        values.put("I_DIMENSION", dimensions);

        return values;
    }

    public String getKeyName() {
        return "I_ID";
    }


}