/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package benchmarks.benchmark;

import benchmarks.DatabaseEngineInterfaces.CassandraInterface;
import benchmarks.helpers.JsonUtil;

import benchmarks.interfaces.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author pedro
 */
public class Benchmark {

    private DataBaseCRUDInterface databaseCRUDClient;
    private DatabaseBenchmarkInterfaceFactory databaseBenchmarkClient;
    private BenchmarkPopulator populator;
    private BenchmarkExecutor executor;
    private ProbabilityDistribution distribution;
    private int MasterPort;


    private int number_threads_populator;
    private long network_delay_populator;
    private int number_threads_executor;
    private long network_delay_executor;  
    private TreeMap<String, String> benchmarkPopulatorInfo;
    private TreeMap<String, String> benchmarkExecutorInfo;
    Map<String, String> probabilityDistributionInfo;
    private Map<String, String> benchmarkExecutorSlaves;

    public static void main(String[] args) {

        boolean populate = false;//Populate
        boolean cleanDB = false; //Full clean
        boolean cleanFB = false; //Clean for benchmark execution
        boolean slave = false;
        boolean master = false;
        for (String arg : args) {

            if(arg.equalsIgnoreCase("-cb")){
                cleanFB =  true;
            }

            if (arg.equalsIgnoreCase("-p")) {
                populate = true;
            }
            if (arg.equalsIgnoreCase("-c")) {
                cleanDB = true;
            }

            if (arg.equalsIgnoreCase("-s")) {
                slave = true;
                if (cleanDB || populate) {
                    System.out.println("SLAVE DOES NOT ALLOW CLEAN OR POPULATION OPTIONS ");
                }
            }
            if (arg.equalsIgnoreCase("-m")) {
                master = true;
            }


        }

        new Benchmark(master, slave, cleanDB, cleanFB , populate);
    }

    public Benchmark(boolean master, boolean slave, boolean cleanDB , boolean cleanFB , boolean populateDatabase) {
        boolean success = loadDescriptor();
        if (!success) {
            System.out.println("ERROR LOADING FILE");
            return;
        }
        run(master, slave, cleanDB, cleanFB,populateDatabase);

    }

    public void run(boolean master, boolean slave, boolean cleanDB,boolean cleanFB, boolean populate) {


        if (slave) {
             BenchmarkSlave slaveHandler = new BenchmarkSlave(MasterPort,executor);
             slaveHandler.run();

        } else {
            if (cleanDB) {
                populator.cleanDB();
            }
            if (populate) {
                populator.populate();
            }
            if(cleanFB){
                if(cleanDB&&populate){
                    System.out.println("[INFO:] BENCHMARK CLEANING IS UNNECESSARY, IGNORED");
                }
                else{
                    populator.BenchmarkClean();
                }
            }

            if (!populate && cleanDB) {
                System.out.println("THE DATABASE IS PROBABLY EMPTY, ABORTING");
                return;
            }

            if (master) {//master, signal slaves
                System.out.println("[INFO:] EXECUTING IN MASTER MODE");
                BenchmarkMaster masterHandler = new BenchmarkMaster(executor,benchmarkExecutorSlaves);
                masterHandler.run();

            } else { //single node run
               System.out.println("[INFO:] EXECUTING IN SINGLE NODE MODE");
               executor.prepare();
               executor.execute("0");
               executor.consolidate(); 
            }

        }

    }

    public boolean loadDescriptor() {
        try {

            FileInputStream in = null;
            String jsonString_r = "";
            try {

                in = new FileInputStream("Benchmark.json");
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
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);

            } finally {
                try {
                    in.close();
                } catch (IOException ex) {
                    Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

            Map<String, Map<String, String>> map = JsonUtil.getMapMapFromJsonString(jsonString_r);


            if (!map.containsKey("BenchmarkInfo")) {
                System.out.println("ERROR: NO INFORMATION ABOUT THE DATA ENGINE FOUND, ABORTING");
                return false;
            } else {

                Map<String, String> databaseInfo = map.get("BenchmarkInfo");
                String databaseClass = databaseInfo.get("DataEngineInterface");
                System.out.println("CHOSEN DATABASE ENGINE: " + databaseClass);
                databaseCRUDClient = (DataBaseCRUDInterface) Class.forName(databaseClass).getConstructor().newInstance();

                String benchmarkInterfaceClass = databaseInfo.get("BenchmarkEngineInterface");
                if (benchmarkInterfaceClass.equals(databaseClass)) {
                    System.out.println("CHOSEN BENCHMARK ENGINE IS EQUAL TO DATABASE ENGINE: SAME OBJECT USED");
                    databaseBenchmarkClient = (DatabaseBenchmarkInterfaceFactory) databaseCRUDClient;
                } else {
                    System.out.println("CHOSEN BENCHMARK ENGINE: " + benchmarkInterfaceClass);
                    databaseBenchmarkClient = (DatabaseBenchmarkInterfaceFactory) Class.forName(benchmarkInterfaceClass).getConstructor().newInstance();
                }


                String populatorClass = databaseInfo.get("BenchmarkPopulator");
                System.out.println("CHOSEN BENCHMARK POPULATOR: " + populatorClass);


                String executorClass = databaseInfo.get("BenchmarkExecutor");
                System.out.println("CHOSEN BENCHMARK EXECUTOR: " + executorClass);


                if (!map.containsKey("BenchmarkPopulator")) {
                    System.out.println("[WARNING:] ONE THREAD USED WHEN POPULATING || OTHER NECESSARY PARAMETERS CAN BE MISSING");
                } else {
                    Map<String, String> info = map.get("BenchmarkPopulator");
                    benchmarkPopulatorInfo = new TreeMap<String, String>();

                    if (!info.containsKey("thread_number")) {
                        number_threads_populator = 1;
                        System.out.println("[WARNING:] ONE THREAD USED WHEN EXECUTING");
                    }
                    if (!info.containsKey("network_delay")) {
                        network_delay_populator= 0;
                        System.out.println("[WARNING:] NO ADDED DELAY WILL BE USED");
                    }

                    for (String s : info.keySet()) {
                        if (s.equalsIgnoreCase("thread_number")) {
                            number_threads_populator = Integer.parseInt(info.get(s).trim());
                        }
                        else if (s.equalsIgnoreCase("network_delay")) {
                            network_delay_populator = Long.parseLong(info.get(s).trim());
                        }
                        else {
                            benchmarkPopulatorInfo.put(s, info.get(s));
                        }
                    }
                }

                if (!map.containsKey("BenchmarkExecutor")) {
                    System.out.println("[WARNING:] ONE THREAD USED WHEN EXECUTING || OTHER NECESSARY PARAMETERS CAN BE MISSING");
                } else {
                    Map<String, String> info = map.get("BenchmarkExecutor");
                    benchmarkExecutorInfo = new TreeMap<String, String>();

                    if (!info.containsKey("thread_number")) {
                        number_threads_executor = 1;
                        System.out.println("[WARNING:] ONE THREAD USED WHEN EXECUTING");
                    }
                    if (!info.containsKey("network_delay")) {
                        network_delay_executor= 0;
                        System.out.println("[WARNING:] NO ADDED DELAY WILL BE USED");
                    }
                    if (!info.containsKey("master_port")) {
                        MasterPort = 55155;
                        System.out.println("[WARNING:] USING DEFAULT MASTER PORT: 55155");
                    }


                    for (String s : info.keySet()) {
                        if (s.equalsIgnoreCase("thread_number")) {
                            number_threads_executor = Integer.parseInt(info.get(s).trim());
                        }
                        else if (s.equalsIgnoreCase("network_delay")) {
                            network_delay_executor = Long.parseLong(info.get(s).trim());
                        }
                        else if (s.equalsIgnoreCase("master_port")) {
                            MasterPort = Integer.parseInt(info.get(s).trim());
                        }
                        else {
                            benchmarkExecutorInfo.put(s, info.get(s));
                        }
                    }
                }


                String distributionClass = "benchmarks.interfaces.ProbabilityDistribution.ZipfDistribution";

                if (!map.containsKey("ProbabilityDistributions")) {
                    System.out.println("[WARNING:] NO DISTRIBUTION INFO FOUND USING NORMAL DISTRIBUTION");
                } else {
                    Map<String, String> info = map.get("ProbabilityDistributions");
                    probabilityDistributionInfo = new TreeMap<String, String>();
                    if (!info.containsKey("Distribution")) {
                        distributionClass = "benchmarks.interfaces.ProbabilityDistribution.ZipfDistribution";
                        System.out.println("[WARNING:] NO DISTRIBUTION INFO FOUND USING NORMAL DISTRIBUTION");
                        probabilityDistributionInfo.put("skew","1");
                    }
                    else{
                        distributionClass = info.get("Distribution");
                        for (String s : info.keySet()) {
                            probabilityDistributionInfo.put(s, info.get(s));
                        }
                    }
                }

                if (!map.containsKey("BenchmarkSlaves")) {
                    System.out.println("[WARNING:] NO SLAVES DEFINED");
                } else {
                    Map<String, String> info = map.get("BenchmarkSlaves");
                    benchmarkExecutorSlaves = info;
                }

                distribution =  (ProbabilityDistribution) Class.forName(distributionClass).getConstructor().newInstance();
                distribution.setInfo(probabilityDistributionInfo);

                populator = (BenchmarkPopulator) Class.forName(populatorClass).getConstructor().newInstance();
                populator.init(databaseCRUDClient,number_threads_populator,network_delay_populator,benchmarkPopulatorInfo);

                executor = (BenchmarkExecutor) Class.forName(executorClass).getConstructor().newInstance();
                executor.init(databaseCRUDClient, databaseBenchmarkClient, number_threads_executor,network_delay_executor, distribution, benchmarkExecutorInfo);
                return true;
            }
        } catch (NoSuchMethodException ex) {
            Logger.getLogger(BenchmarkPopulator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SecurityException ex) {
            Logger.getLogger(BenchmarkPopulator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            Logger.getLogger(BenchmarkPopulator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(BenchmarkPopulator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalArgumentException ex) {
            Logger.getLogger(BenchmarkPopulator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InvocationTargetException ex) {
            Logger.getLogger(BenchmarkPopulator.class.getName()).log(Level.SEVERE, null, ex);

        } catch (ClassNotFoundException ex) {
            Logger.getLogger(BenchmarkPopulator.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("ERROR: THERE IS SOME PROBLEM WITH THE DEFINITIONS FILE OR THE LOADED INTERFACES");
        return false;
    }

}
