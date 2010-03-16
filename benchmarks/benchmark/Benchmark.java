/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package benchmarks.benchmark;

import benchmarks.DatabaseEngineInterfaces.CassandraInterface;
import benchmarks.helpers.JsonUtil;

import benchmarks.interfaces.BenchmarkExecuter;
import benchmarks.interfaces.BenchmarkInterfaceFactory;
import benchmarks.interfaces.CRUD;
import benchmarks.interfaces.BenchmarkPopulator;
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
 *
 * @author pedro
 */
public class Benchmark {

    private CRUD databaseClient;
    private BenchmarkInterfaceFactory benchmarkClient;
    private BenchmarkPopulator populator;
    private BenchmarkExecuter executer;
    private int number_threads_populator;
    private int number_threads_executer;
    private TreeMap<String,String> benchmarkPopulatorInfo;
    private TreeMap<String,String> benchmarkExecuterInfo;

    public static void main(String[] args) {
        new Benchmark();
    }


    public Benchmark() {
       boolean sucess = loadDescriptor();
       if(!sucess){
            System.out.println("ERROR LOADING FILE");
            return;
       }
       run();

    }
    
    public void run(){
        populator.populate();
        executer.execute(populator.getUseFullData());
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
            }
            else {

                Map<String, String> databaseInfo = map.get("BenchmarkInfo");
                String databaseClass = databaseInfo.get("DataEngineInterface");
                System.out.println("CHOSEN DATABASE ENGINE: " + databaseClass);
                databaseClient = (CRUD) Class.forName(databaseClass).getConstructor().newInstance();

                String benchmarkInterfaceClass = databaseInfo.get("BenchmarkEngineInterface");
                if(benchmarkInterfaceClass.equals(databaseClass)){
                    System.out.println("CHOSEN BENCHMARK ENGINE IS EQUAL TO DATABASE ENGINE: SAME OBJECT USED");
                    benchmarkClient  = (BenchmarkInterfaceFactory) databaseClient;
                }
                else{
                    System.out.println("CHOSEN BENCHMARK ENGINE: " + benchmarkInterfaceClass);
                    benchmarkClient = (BenchmarkInterfaceFactory) Class.forName(benchmarkInterfaceClass).getConstructor().newInstance();
                }



                String populatorClass = databaseInfo.get("BenchmarkPopulator");
                System.out.println("CHOSEN BENCHMARK POPULATOR: " + populatorClass);
                

                String executorClass = databaseInfo.get("BenchmarkExecuter");
                System.out.println("CHOSEN BENCHMARK EXECUTOR: " +  executorClass);
                

                if (!map.containsKey("BenchmarkPopulator")) {
                    System.out.println("WARNING: ONE THREAD USED WHEN POPULATING || OTHER NECESSARY PARAMETERS CAN BE MISSING");
                }
                else {
                    Map<String, String> info = map.get("BenchmarkPopulator");
                    benchmarkPopulatorInfo =  new TreeMap<String, String>();
                   
                    if(!info.containsKey("thread_number")){
                        number_threads_populator = 1;
                        System.out.println("WARNING: ONE THREAD USED WHEN EXECUTING");
                    }

                    for(String s : info.keySet()){
                        if(s.equalsIgnoreCase("thread_number")){
                            number_threads_populator = Integer.parseInt(info.get(s).trim());
                        }
                        else{
                            benchmarkPopulatorInfo.put(s, info.get(s));
                        }
                    }
                }
                if (!map.containsKey("BenchmarkExecuter")) {
                    System.out.println("WARNING: ONE THREAD USED WHEN EXECUTING || OTHER NECESSARY PARAMETERS CAN BE MISSING");
                }
                else {
                    Map<String, String> info = map.get("BenchmarkExecuter");
                    benchmarkExecuterInfo = new TreeMap<String, String>();
                    
                    if(!info.containsKey("thread_number")){
                        number_threads_executer = 1;
                        System.out.println("WARNING: ONE THREAD USED WHEN EXECUTING");
                    }

                    for(String s : info.keySet()){
                        if(s.equalsIgnoreCase("thread_number")){
                            number_threads_executer = Integer.parseInt(info.get(s).trim());
                        }
                        else{
                            benchmarkExecuterInfo.put(s, info.get(s));
                        }
                    }
                }

                Object[] args = new Object[]{databaseClient,number_threads_populator,benchmarkPopulatorInfo};
                Class[] args_cl = new Class[]{CRUD.class,int.class,Map.class};
                populator = (BenchmarkPopulator) Class.forName(populatorClass).getConstructor(args_cl).newInstance(args);


                args_cl = new Class[]{CRUD.class, BenchmarkInterfaceFactory.class,int.class,Map.class};
                args = new Object[]{databaseClient,benchmarkClient,number_threads_executer,benchmarkExecuterInfo};
                executer = (BenchmarkExecuter) Class.forName(executorClass).getConstructor(args_cl).newInstance(args);

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
