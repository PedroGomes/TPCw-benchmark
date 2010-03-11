/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package benchmarks.helpers;

import benchmarks.DatabaseEngineInterfaces.CassandraInterface;
import com.sun.tools.javac.jvm.ClassWriter.StringOverflow;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author pedro
 */
public class FileCreator {

    public static void main(String[] args) throws IOException {

  //       Convert map to JSON string


        String jsonString = "";

        Map<String, String> CassandraPaths = new HashMap<String, String>();

        CassandraPaths.put("Order", "id");

        CassandraPaths.put("Cart", "value");



        Map<String, String> cassandarConsitency = new HashMap<String, String>();

        cassandarConsitency.put("Order", "1");

        cassandarConsitency.put("Cart", "0");

        Map<String, Map<String, String>> properties = new HashMap<String, Map<String, String>>();
        properties.put("BenchmarkInfo", CassandraPaths);


        List<Map<String, String>> mapList = new ArrayList<Map<String, String>>();
        mapList.add(CassandraPaths);
        mapList.add(cassandarConsitency);

        // Convert to json string

        jsonString = JsonUtil.getJsonStringFromMapMap(properties);




        FileOutputStream out = null;
        try {
            Properties defaultProps = new Properties();
            out = new FileOutputStream("Benchmark.xml");
            BufferedOutputStream stream = new BufferedOutputStream(out);
            jsonString = jsonString.replace(":{", ":{\n\t");
            jsonString = jsonString.replace(",", ",\n\t");
            jsonString = jsonString.replace("},\n\t", "},\n");
            jsonString = jsonString.replace("}}", "}\n}");
            jsonString = jsonString.replace("{\"", "{\n\"");

            stream.write(jsonString.getBytes());

            stream.flush();
            stream.close();
            out.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                out.close();
            } catch (IOException ex) {
                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

//String jsonString_r ="";
//        FileInputStream in = null;
//        try {
//
//            in = new FileInputStream("CassandraDB.xml");
//            BufferedReader bin = new BufferedReader(new InputStreamReader(in));
//            String s = "";
//            StringBuilder sb = new StringBuilder();
//            while(s!=null){
//                sb.append(s);
//                s = bin.readLine();
//            }
//
//
//            jsonString_r = sb.toString().replace("\n", "");
//            bin.close();
//            in.close();
//
//        } catch (FileNotFoundException ex) {
//            Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
//        } finally {
//            try {
//                in.close();
//            } catch (IOException ex) {
//                Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
//            }
//        }
//
//
//        Map<String,Map<String,String>> map = JsonUtil.getMapMapFromJsonString(jsonString_r);
//        for(String s : map.keySet()){
//            System.out.println("Value"+s);
//        }
//
    }
}
