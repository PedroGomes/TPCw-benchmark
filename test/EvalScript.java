/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import benchmarks.TpcwBenchmark.TPCWBenchmarkInterface.BuyingResult;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.script.*;
import org.apache.cassandra.service.ConsistencyLevel;

/**
 *
 * @author pedro
 */
public class EvalScript {



        public static void main(String[] args) throws Exception {
                    new Solver();


//          ScriptEngineManager mgr = new ScriptEngineManager();
//  List<ScriptEngineFactory> factories =
//      mgr.getEngineFactories();
//  for (ScriptEngineFactory factory: factories) {
//    System.out.println("ScriptEngineFactory Info");
//    String engName = factory.getEngineName();
//    String engVersion = factory.getEngineVersion();
//    String langName = factory.getLanguageName();
//    String langVersion = factory.getLanguageVersion();
//    System.out.printf("\tScript Engine: %s (%s)\n",
//        engName, engVersion);
//    List<String> engNames = factory.getNames();
//    for(String name: engNames) {
//      System.out.printf("\tEngine Alias: %s\n", name);
//    }
//    System.out.printf("\tLanguage: %s (%s)\n",
//        langName, langVersion);
        }
//        // create a script engine manager
//        ScriptEngineManager factory = new ScriptEngineManager();
//        // create a JavaScript engine
//        ScriptEngine engine = factory.getEngineByName("JavaScript");
//        // evaluate JavaScript code from String
//        engine.eval("print('Hello, World')");
        // }
    }


class Solver {
   final int N;
   final float[][] data;
   final CyclicBarrier barrier;

        private void waitUntilDone() {
            System.out.println("Ja esta");
        }

   class Worker implements Runnable {
     int myRow;
     Worker(int row) { myRow = row; }
     public void run() {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException ex) {
                    Logger.getLogger(EvalScript.class.getName()).log(Level.SEVERE, null, ex);
                }
                System.out.println("Waiting");
         try {
           barrier.await();
         } catch (InterruptedException ex) {
           return;
         } catch (BrokenBarrierException ex) {
           return;
         }
       }
     }


   public Solver() {
      data = null;
     N = 4;
     barrier = new CyclicBarrier(N+1);
     for (int i = 0; i < N; ++i)
       new Thread(new Worker(i)).start();
        try {
            barrier.await();
        } catch (InterruptedException ex) {
            Logger.getLogger(Solver.class.getName()).log(Level.SEVERE, null, ex);
        } catch (BrokenBarrierException ex) {
            Logger.getLogger(Solver.class.getName()).log(Level.SEVERE, null, ex);
        }
     waitUntilDone();
   }
 }