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

public class Item implements Entity {

	public int I_ID; // ID
	public String I_TITLE;
	public String I_PUB_DATE;
	public int I_A_ID;
	public String I_PUBLISHER;
	public String I_DESC;
	public String I_SUBJECT;
	public String I_THUMBNAIL;
	public String I_IMAGE;
	public double I_COST;
	public int I_STOCK;
	public String I_ISBN;// international id
	public double I_SRP;// Suggested Retail Price
	public int I_RELATED1;
	public int I_RELATED2;
	public int I_RELATED3;
	public int I_RELATED4;
	public int I_RELATED5;
	public int I_PAGE;
	public int I_TOTAL_SOLD;
	public String I_AVAIL; // Data when available
	public String I_BACKING;
	public String I_DIMENSION;

	public Item(int i_id, String I_TITLE, String pubDate, String I_PUBLISHER,
			String I_DESC, String I_SUBJECT, String thumbnail, String image,
			double I_COST, int I_STOCK, String isbn, double srp,
			int I_RELATED1, int I_RELATED2, int I_RELATED3, int I_RELATED4,
			int I_RELATED5, int I_PAGE, String avail, String I_BACKING,
			String dimensions, int author) {
		this.I_ID = i_id;
		this.I_TITLE = I_TITLE;
		this.I_PUB_DATE = pubDate;
		this.I_A_ID = author;
		this.I_PUBLISHER = I_PUBLISHER;
		this.I_DESC = I_DESC;
		this.I_SUBJECT = I_SUBJECT;
		this.I_THUMBNAIL = thumbnail;
		this.I_IMAGE = image;
		this.I_COST = I_COST;
		this.I_STOCK = I_STOCK;
		this.I_ISBN = isbn;
		this.I_SRP = srp;
		this.I_RELATED1 = I_RELATED1;
		this.I_RELATED2 = I_RELATED2;
		this.I_RELATED3 = I_RELATED3;
		this.I_RELATED4 = I_RELATED4;
		this.I_RELATED5 = I_RELATED5;
		this.I_PAGE = I_PAGE;
		this.I_AVAIL = avail;
		this.I_BACKING = I_BACKING;
		this.I_DIMENSION = dimensions;

	}

	public int getI_AUTHOR() {
		return I_A_ID;
	}

	public void setI_AUTHOR(int i_AUTHOR) {
		I_A_ID = i_AUTHOR;
	}

	public String getI_BACKING() {
		return I_BACKING;
	}

	public void setI_BACKING(String I_BACKING) {
		this.I_BACKING = I_BACKING;
	}

	public double getI_COST() {
		return I_COST;
	}

	public void setI_COST(double i_COST) {
		I_COST = i_COST;
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

	public int getI_STOCK() {
		return I_STOCK;
	}

	public void setI_STOCK(int i_STOCK) {
		I_STOCK = i_STOCK;
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

	public String getAvail() {
		return I_AVAIL;
	}

	public void setAvail(String avail) {
		this.I_AVAIL = avail;
	}

	public String getDimensions() {
		return I_DIMENSION;
	}

	public void setDimensions(String dimensions) {
		this.I_DIMENSION = dimensions;
	}

	public int getI_id() {
		return I_ID;
	}

	public void setI_id(int i_id) {
		this.I_ID = i_id;
	}

	public String getImage() {
		return I_IMAGE;
	}

	public void setImage(String image) {
		this.I_IMAGE = image;
	}

	public String getIsbn() {
		return I_ISBN;
	}

	public void setIsbn(String isbn) {
		this.I_ISBN = isbn;
	}

	public String getPubDate() {
		return I_PUB_DATE;
	}

	public void setPubDate(String pubDate) {
		this.I_PUB_DATE = pubDate;
	}

	public double getI_SRP() {
		return I_SRP;
	}

	public void setSrp(double srp) {
		this.I_SRP = srp;
	}

	public String getThumbnail() {
		return I_THUMBNAIL;
	}

	public void setThumbnail(String thumbnail) {
		this.I_THUMBNAIL = thumbnail;
	}

	public TreeMap<String, Object> getValuesToInsert() {

		TreeMap<String, Object> values = new TreeMap<String, Object>();

		values.put("I_TITLE", I_TITLE);
		values.put("I_A_ID", I_A_ID);
		values.put("I_PUB_DATE", I_PUB_DATE);
		values.put("I_PUBLISHER", I_PUBLISHER);
		values.put("I_SUBJECT", I_SUBJECT);
		values.put("I_DESC", I_DESC);
		values.put("I_RELATED1", I_RELATED1);
		values.put("I_RELATED2", I_RELATED2);
		values.put("I_RELATED3", I_RELATED3);
		values.put("I_RELATED4", I_RELATED4);
		values.put("I_RELATED5", I_RELATED5);
		values.put("I_THUMBNAIL", I_THUMBNAIL);
		values.put("I_IMAGE", I_IMAGE);
		values.put("I_SRP", I_SRP);
		values.put("I_COST", I_COST);
		values.put("I_AVAIL", I_AVAIL);
		values.put("I_STOCK", I_STOCK);
		values.put("I_ISBN", I_ISBN);
		values.put("I_PAGE", I_PAGE);
		values.put("I_BACKING", I_BACKING);
		values.put("I_DIMENSION", I_DIMENSION);
		values.put("I_ID", I_ID);

		return values;
	}

	public String getKeyName() {
		return "I_ID";
	}
	
	public String getI_PUB_DATE() {
		return I_PUB_DATE;
	}

	public void setI_PUB_DATE(String i_PUB_DATE) {
		I_PUB_DATE = i_PUB_DATE;
	}

	public int getI_ID() {
		return I_ID;
	}

	public void setI_ID(int i_ID) {
		I_ID = i_ID;
	}

	public int getI_A_ID() {
		return I_A_ID;
	}

	public void setI_A_ID(int i_A_ID) {
		I_A_ID = i_A_ID;
	}

	public String getI_THUMBNAIL() {
		return I_THUMBNAIL;
	}

	public void setI_THUMBNAIL(String i_THUMBNAIL) {
		I_THUMBNAIL = i_THUMBNAIL;
	}

	public String getI_IMAGE() {
		return I_IMAGE;
	}

	public void setI_IMAGE(String i_IMAGE) {
		I_IMAGE = i_IMAGE;
	}

	public String getI_ISBN() {
		return I_ISBN;
	}

	public void setI_ISBN(String i_ISBN) {
		I_ISBN = i_ISBN;
	}

	public int getI_RELATED1() {
		return I_RELATED1;
	}

	public void setI_RELATED1(int i_RELATED1) {
		I_RELATED1 = i_RELATED1;
	}

	public int getI_RELATED2() {
		return I_RELATED2;
	}

	public void setI_RELATED2(int i_RELATED2) {
		I_RELATED2 = i_RELATED2;
	}

	public int getI_RELATED3() {
		return I_RELATED3;
	}

	public void setI_RELATED3(int i_RELATED3) {
		I_RELATED3 = i_RELATED3;
	}

	public int getI_RELATED4() {
		return I_RELATED4;
	}

	public void setI_RELATED4(int i_RELATED4) {
		I_RELATED4 = i_RELATED4;
	}

	public int getI_RELATED5() {
		return I_RELATED5;
	}

	public void setI_RELATED5(int i_RELATED5) {
		I_RELATED5 = i_RELATED5;
	}

	public String getI_AVAIL() {
		return I_AVAIL;
	}

	public void setI_AVAIL(String i_AVAIL) {
		I_AVAIL = i_AVAIL;
	}

	public String getI_DIMENSION() {
		return I_DIMENSION;
	}

	public void setI_DIMENSION(String i_DIMENSION) {
		I_DIMENSION = i_DIMENSION;
	}

	public void setI_SRP(double i_SRP) {
		I_SRP = i_SRP;
	}

	public int getI_TOTAL_SOLD() {
		return I_TOTAL_SOLD;
	}

	public void setI_TOTAL_SOLD(int i_TOTAL_SOLD) {
		I_TOTAL_SOLD = i_TOTAL_SOLD;
	}

	

}