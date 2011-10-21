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

import java.util.LinkedList;
import java.util.List;

public class BestSellerSubject {

	List<BestSellerEntry> entries;
	
	public BestSellerSubject(){
		entries = new LinkedList<BestSellerEntry>();
	}

	public List<BestSellerEntry> getEntries() {
		return entries;
	}

	public void setEntries(List<BestSellerEntry> entries) {
		this.entries = entries;
	}
		
}
