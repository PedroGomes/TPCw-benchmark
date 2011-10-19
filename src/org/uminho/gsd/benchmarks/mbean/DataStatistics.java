/*
 * *********************************************************************
 * Copyright (c) 2010 Pedro Gomes and Universidade do Minho.
 * All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************
 */

package org.uminho.gsd.benchmarks.mbean;


import org.uminho.gsd.benchmarks.dataStatistics.ResultHandler;
import org.uminho.gsd.benchmarks.helpers.Pair;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class DataStatistics implements DataStatisticsMBean{

    /**
     * The existing result handlers that measure the clients throughput and latencies
     */
    private List<ResultHandler> client_resultHandlers;

	 /**
     * The existing result handler that is used in a global way to count tpm.
     */
    private ResultHandler tpm_resultHandler;


	/**
	 *The home operation latency
	 **/
	private long homeLatency;


	/**
	 *The best sellers operation latency
	 **/
	private long bestSellersLatency;


	/**
	 *The product detail operation latency
	 **/
	private long itemInfoLatency;

	/**
	 *The shopping cart operation latency
	 **/
	private long shoppingCartLatency;


	/**
	 *The number of transactions per minute
	 **/
	private long transactionsPerMinute;




	/**
     * The timer class for statistics extraction
     */
    private Timer data_extraction = new Timer("Statistics calculation", true);


	/**
	 * The statistic data calculation timeout
	 */
	private long statistics_timeout = 60000;

	TimerTask metric_calculation = new TimerTask() {

		public void run() {
			homeLatency = calculateLatency("OP_HOME");
			bestSellersLatency = calculateLatency("OP_BEST_SELLERS");
			itemInfoLatency = calculateLatency("OP_ITEM_INFO");
			shoppingCartLatency = calculateLatency("OP_SHOPPING_CART");
			transactionsPerMinute = calculateTransactionsPerMinute();
		}
	};



	public DataStatistics() {
		try {
			// register jmx bean
			ObjectName name = new ObjectName("org.uminho.gsd.benchmarks.mbean:type=DataStatistics");
			ManagementFactory.getPlatformMBeanServer().registerMBean(this, name);
		} catch (Throwable t) {
			t.printStackTrace();
		}
		this.client_resultHandlers = ResultHandler.getClient_results();
		data_extraction.schedule(metric_calculation, 15000, statistics_timeout);
	}

	public void setTpm_resultHandler(ResultHandler tpm_resultHandler) {
		this.tpm_resultHandler = tpm_resultHandler;
	}

	@Override
	public long getHomeLatency() {
		return homeLatency;
	}

	@Override
	public long getBestSellersLatency() {
		return bestSellersLatency;
	}

	@Override
	public long getItemInfoLatency() {
		return itemInfoLatency;
	}

	@Override
	public long getShoppingCartLatency() {
		return shoppingCartLatency;
	}

	@Override
	public long getTransactionsPerMinute() {
		return transactionsPerMinute;
	}

	public long calculateLatency(String operation) {
		int num_results = 0;
        int total = 0;

        long current_time = System.currentTimeMillis();
        //1000*60

        for (ResultHandler handler : client_resultHandlers) {
            if (!handler.getTime_results().containsKey(operation)) {
                continue;
            }

            ArrayList<Pair<Long, Long>> results = handler.getTime_results().get(operation);
            int size = results.size();
            boolean ended = false;

            if (size == 0) {
                continue;
            }

            int reversed_index = 1;
            while (!ended) {
                Pair<Long, Long> pair = results.get(size - reversed_index);
                long time = pair.getLeft();
                if (time > current_time - (statistics_timeout)) {
                    total += pair.getRight();
                    num_results++;
                } else {
                    ended = true;
                }
                if (reversed_index == (size)) {
                    ended = true;
                }
                reversed_index++;

            }
        }

        if (num_results == 0)
            return 0;
        long result = total / num_results;
        return result;
	}

	public long calculateTransactionsPerMinute() {
		 	int num_results = 0;
        int total = 0;

        long current_time = System.currentTimeMillis();
        //1000*60

        if (tpm_resultHandler!=null) {
            if (!tpm_resultHandler.getTime_results().containsKey("TPM")) {
				return 0;
            }

            ArrayList<Pair<Long, Long>> results = tpm_resultHandler.getTime_results().get("TPM");
            int size = results.size();
            boolean ended = false;

            if (size == 0) {
                return 0;
            }

            int reversed_index = 1;
            while (!ended) {
                Pair<Long, Long> pair = results.get(size - reversed_index);
                long time = pair.getLeft();
                if (time > current_time - (statistics_timeout)) {
                    total += pair.getRight();
                    num_results++;
                } else {
                    ended = true;
                }
                if (reversed_index == (size)) {
                    ended = true;
                }
                reversed_index++;

            }
        }

        if (num_results == 0)
            return 0;
        long result = total / num_results;
        return result;
	}




}
