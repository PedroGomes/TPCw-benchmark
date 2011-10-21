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

import java.util.TreeMap;

import org.uminho.gsd.benchmarks.interfaces.Entity;

public class SCLine implements Entity {

	public int SCL_QTY; 
	public int I_ID;
	
	public double SCL_COST;
	public double SCL_SRP;
	public String SCL_TITLE;
	public String SCL_BACKING;
	
	
	public int getI_ID() {
		return I_ID;
	}

	public void setI_ID(int i_ID) {
		I_ID = i_ID;
	}

	public double getSCL_COST() {
		return SCL_COST;
	}

	public void setSCL_COST(double sCL_COST) {
		SCL_COST = sCL_COST;
	}

	public double getSCL_SRP() {
		return SCL_SRP;
	}

	public void setSCL_SRP(double sCL_SRP) {
		SCL_SRP = sCL_SRP;
	}

	public String getSCL_TITLE() {
		return SCL_TITLE;
	}

	public void setSCL_TITLE(String sCL_TITLE) {
		SCL_TITLE = sCL_TITLE;
	}

	public String getSCL_BACKING() {
		return SCL_BACKING;
	}

	public void setSCL_BACKING(String sCL_BACKING) {
		SCL_BACKING = sCL_BACKING;
	}

	public SCLine(int I_ID){
		this.I_ID = I_ID;
	}
	
	public String getKeyName() {
		return "I_ID";
	}

	@Override
	public TreeMap<String, Object> getValuesToInsert() {
		TreeMap<String, Object> list =  new TreeMap<String, Object>();
		list.put("I_ID", I_ID);
		list.put("SCL_QTY", SCL_QTY);
		list.put("SCL_COST", SCL_COST);
		list.put("SCL_SRP", SCL_SRP);
		list.put("SCL_TITLE", SCL_TITLE);
		list.put("SCL_BACKING", SCL_BACKING);
		return list;
	}
	
	public int getItemId(){
		return I_ID;
	}

	public int getSCL_QTY() {
		return SCL_QTY;
	}

	public void setSCL_QTY(int qTY) {
		SCL_QTY = qTY;
	}

}
