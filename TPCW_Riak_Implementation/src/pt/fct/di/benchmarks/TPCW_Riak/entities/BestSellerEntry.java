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

public class BestSellerEntry {

	public String I_SUBJECT;
	public int I_ID;
	public int I_TOTAL_SOLD;
		
	public BestSellerEntry(String i_SUBJECT, int i_ID, int i_TOTAL_SOLD) {
		super();
		I_SUBJECT = i_SUBJECT;
		I_ID = i_ID;
		I_TOTAL_SOLD = i_TOTAL_SOLD;
	}
	
	public String getI_SUBJECT() {
		return I_SUBJECT;
	}
	public void setI_SUBJECT(String i_SUBJECT) {
		I_SUBJECT = i_SUBJECT;
	}
	public int getI_ID() {
		return I_ID;
	}
	public void setI_ID(int i_ID) {
		I_ID = i_ID;
	}
	public int getI_TOTAL_SOLD() {
		return I_TOTAL_SOLD;
	}
	public void setI_TOTAL_SOLD(int i_TOTAL_SOLD) {
		I_TOTAL_SOLD = i_TOTAL_SOLD;
	}
	
	
}
