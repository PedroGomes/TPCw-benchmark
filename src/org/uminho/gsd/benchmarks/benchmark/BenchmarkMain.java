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

package org.uminho.gsd.benchmarks.benchmark;


import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.uminho.gsd.benchmarks.helpers.JsonUtil;
import org.uminho.gsd.benchmarks.interfaces.executor.AbstractDatabaseExecutorFactory;
import org.uminho.gsd.benchmarks.interfaces.populator.AbstractBenchmarkPopulator;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;


public class BenchmarkMain {

    private static Logger logger = Logger.getLogger(BenchmarkMain.class);

    private BenchmarkExecutor executor;

    private AbstractBenchmarkPopulator populator;
    private Class worload;
    private Class databaseExecutor;

    private static BenchmarkNodeID id;

    //Files
    private String populator_conf;
    private String executor_conf;
    private String workload_conf;


    private static int SlavePort;


    private int number_threads;
    private int operation_number;

    private Map<String, String> benchmarkExecutorSlaves;

    public static void main(String[] args) {

        boolean populate = false;//Populate
        boolean cleanDB = false; //Full clean
        boolean cleanFB = false; //Clean for benchmark execution
        boolean slave = false;
        boolean master = false;
        boolean ocp = false; //only clean and populate

        initLogger();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equalsIgnoreCase("-cb")) {
                cleanFB = true;
            } else if (arg.equalsIgnoreCase("-p")) {
                populate = true;
            } else if (arg.equalsIgnoreCase("-ocp")) {
                ocp = true;
            } else if (arg.equalsIgnoreCase("-c")) {
                cleanDB = true;
            } else if (arg.equalsIgnoreCase("-s")) {

                slave = true;

                if ((i + 1) != args.length) {

                    try {
                        SlavePort = Integer.parseInt(args[i + 1]);
                    }
                    catch (Exception e) {
                        System.out.println("[ERROR:] ERROR PARSING SLAVE PORT");
                        return;
                    }
                    i++;
                } else {
                    System.out.println("[ERROR:] SLAVE WITH NO AVAILABLE PORT");
                    return;

                }

                if (cleanDB || populate) {
                    logger.debug("SLAVE DOES NOT ALLOW CLEAN OR POPULATION OPTIONS ");
                }
            } else if (arg.equalsIgnoreCase("-m")) {
                master = true;
            } else {
                logger.debug("[WARNING:] OPTION NOT RECOGNIZED: " + arg);
            }
        }


        new BenchmarkMain(master, slave, cleanDB, cleanFB, populate,ocp);
    }

    public BenchmarkMain(boolean master, boolean slave, boolean cleanDB, boolean cleanFB, boolean populateDatabase,boolean cap) {
        boolean success = loadDescriptor();
        if (!success) {
            logger.debug("ERROR LOADING FILE");
            return;
        }
        run(master, slave, cleanDB, cleanFB, populateDatabase,cap);

    }

    public void run(boolean master, boolean slave, boolean cleanDB, boolean cleanFB, boolean populate,boolean cap) {


        if (slave) {
            BenchmarkSlave slaveHandler = new BenchmarkSlave(SlavePort, executor);
            slaveHandler.run();

        } else {

            if (cap) {
                populator.cleanDB();
                populator.populate();
                return;
            }

            if (cleanDB) {
                populator.cleanDB();
            }
            if (populate) {
                populator.populate();
            }
            if (cleanFB) {
                if (cleanDB && populate) {
                    logger.debug("[INFO:] BENCHMARK CLEANING IS UNNECESSARY, IGNORED");
                } else {
                    populator.BenchmarkClean();
                }
            }

            if (!populate && cleanDB) {
                logger.debug("THE DATABASE IS PROBABLY EMPTY, ABORTING");
                return;
            }

            if (master) {//master, signal slaves
                logger.debug("[INFO:] EXECUTING IN MASTER MODE");
                BenchmarkMaster masterHandler = new BenchmarkMaster(executor, benchmarkExecutorSlaves);
                masterHandler.run();

            } else { //single node run
                logger.debug("[INFO:] EXECUTING IN SINGLE NODE MODE");
                executor.prepare();
                executor.run(new BenchmarkNodeID(1));
                executor.consolidate();
            }


        }

    }

    public boolean loadDescriptor() {
        try {

            FileInputStream in = null;
            String jsonString_r = "";
            try {

                in = new FileInputStream("conf/Benchmark.json");
                BufferedReader bin = new BufferedReader(new InputStreamReader(in));
                String s = "";
                StringBuilder sb = new StringBuilder();
                while (s != null) {
                    sb.append(s);
                    s = bin.readLine();
                }
                jsonString_r = sb.toString().replace("\n", "");
                bin.close();
                in.close();

            } catch (FileNotFoundException ex) {
                logger.error("", ex);
            } catch (IOException ex) {
                logger.error("", ex);

            } finally {
                try {
                    in.close();
                } catch (IOException ex) {
                    logger.error("", ex);
                }
            }

            Map<String, Map<String, String>> map = JsonUtil.getMapMapFromJsonString(jsonString_r);


            if (!map.containsKey("BenchmarkInterfaces")) {
                logger.debug("[ERROR:] NO INFORMATION ABOUT THE DATA ENGINE FOUND, ABORTING");
                return false;
            } else {

                Map<String, String> databaseInfo = map.get("BenchmarkInterfaces");
                String databaseClass = databaseInfo.get("DataEngineInterface");
                if (databaseClass == null || databaseClass.isEmpty()) {
                    logger.debug("[ERROR:] NO INFORMATION ABOuT THE DATA ENGINE EXECUTOR");
                    return false;
                }

                logger.debug("CHOSEN DATABASE ENGINE: " + databaseClass);
                databaseExecutor = Class.forName(databaseClass);

                String benchmarkInterfaceClass = databaseInfo.get("BenchmarkWorkload");
                if (benchmarkInterfaceClass == null || benchmarkInterfaceClass.isEmpty()) {
                    logger.debug("[ERROR:] NO INFORMATION ABOuT THE WORKLOAD GENERATOR");
                    return false;
                }
                worload = Class.forName(benchmarkInterfaceClass);


                String populatorClass = databaseInfo.get("BenchmarkPopulator");
                if (populatorClass == null || populatorClass.isEmpty()) {
                    logger.debug("[ERROR:] NO INFORMATION ABOUT THE POPULATOR");
                    return false;
                }
                logger.debug("CHOSEN BENCHMARK POPULATOR: " + populatorClass);


                if (!map.containsKey("BenchmarkInfo")) {
                    logger.debug("[ERROR] NO CONFIGURATION FILES INFO FOUND");
                    return false;
                } else {
                    Map<String, String> info = map.get("BenchmarkInfo");
                    populator_conf = info.get("populatorConfiguration");
                    if (populator_conf == null || populator_conf.isEmpty()) {
                        logger.debug("[ERROR:] NO CONFIGURATION FILE FOR POPULATOR");
                        return false;
                    }

                    executor_conf = info.get("databaseExecutorConfiguration");
                    if (executor_conf == null || executor_conf.isEmpty()) {
                        logger.debug("[ERROR:] NO CONFIGURATION FILE FOR DATABASE EXECUTOR");
                        return false;
                    }

                    workload_conf = info.get("workloadConfiguration");
                    if (workload_conf == null || workload_conf.isEmpty()) {
                        logger.debug("[ERROR:] NO CONFIGURATION FILE FOR WORKLOAD");
                        return false;
                    }

                    if (!info.containsKey("thread_number")) {
                        number_threads = 1;
                        logger.debug("[WARNING:] ONE THREAD USED WHEN EXECUTING");
                    } else {
                        number_threads = Integer.parseInt(info.get("thread_number"));

                    }

                    if (!info.containsKey("operation_number")) {
                        operation_number = 1000;
                        logger.debug("[WARNING:] 1000 OPERATION EXECUTED AS DEFAULT");
                    } else {

                        operation_number = Integer.parseInt(info.get("operation_number"));
                        logger.debug("[INFO:] NUMBER OF OPERATIONS -> " + operation_number);
                    }


                }


                if (!map.containsKey("BenchmarkSlaves")) {
                    logger.debug("[WARNING:] NO SLAVES DEFINED");
                } else {
                    Map<String, String> info = map.get("BenchmarkSlaves");
                    benchmarkExecutorSlaves = info;
                }


                executor = new BenchmarkExecutor(worload, workload_conf, databaseExecutor, executor_conf, operation_number, number_threads);

                populator = (AbstractBenchmarkPopulator) Class.forName(populatorClass).getConstructor(AbstractDatabaseExecutorFactory.class, String.class).newInstance(executor.getDatabaseInterface(), populator_conf);

                return true;
            }
        } catch (NoSuchMethodException ex) {
            logger.error("", ex);
        } catch (SecurityException ex) {
            logger.error("", ex);
        } catch (InstantiationException ex) {
            logger.error("", ex);
        } catch (IllegalAccessException ex) {
            logger.error("", ex);
        } catch (IllegalArgumentException ex) {
            logger.error("", ex);
        } catch (InvocationTargetException ex) {
            logger.error("", ex);

        } catch (ClassNotFoundException ex) {
            logger.error("", ex);
        }
        logger.debug("ERROR: THERE IS SOME PROBLEM WITH THE DEFINITIONS FILE OR THE LOADED INTERFACES");
        return false;
    }

    public static void initLogger() {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.ERROR);//INFO
    }

}
