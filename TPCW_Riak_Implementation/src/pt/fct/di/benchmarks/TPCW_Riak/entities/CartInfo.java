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

public class CartInfo {

	private String SC_DATE;
	private float SC_TAX;
	private float SC_TOTAL;
	private float SC_SHIP_COST;
	private float SC_SUB_TOTAL;


	public float getSC_TAX() {
		return SC_TAX;
	}

	public float getSC_SUB_TOTAL() {
		return SC_SUB_TOTAL;
	}

	public void setSC_SUB_TOTAL(float sC_SUB_TOTAL) {
		SC_SUB_TOTAL = sC_SUB_TOTAL;
	}

	public void setSC_TAX(float sC_TAX) {
		SC_TAX = sC_TAX;
	}

	public float getSC_TOTAL() {
		return SC_TOTAL;
	}

	public void setSC_TOTAL(float sC_TOTAL) {
		SC_TOTAL = sC_TOTAL;
	}

	public float getSC_SHIP_COST() {
		return SC_SHIP_COST;
	}

	public void setSC_SHIP_COST(float sC_SHIP_COST) {
		SC_SHIP_COST = sC_SHIP_COST;
	}

	public String getSC_DATE() {
		return SC_DATE;
	}

	public void setSC_DATE(String sC_DATE) {
		SC_DATE = sC_DATE;
	}
	
	
	
}
