package benchmarks.TpcwBenchmark;

import benchmarks.interfaces.Entity;

import java.util.TreeMap;

/**
 * Created by IntelliJ IDEA.
 * User: pedro
 * Date: Mar 26, 2010
 * Time: 3:39:14 AM
 * To change this template use File | Settings | File Templates.
 */
public class Results implements Entity{

    int Bought;
    int TotalStock;
    String ClientID;

    public Results(int bought, int totalStock, String clientID) {
        Bought = bought;
        TotalStock = totalStock;
        ClientID = clientID;
    }

    public String getKeyName() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public TreeMap<String, Object> getValuesToInsert() {
         TreeMap<String, Object> values =  new TreeMap<String, Object>();

         values.put("BOUGHT", Bought);
         values.put("STOCK", TotalStock);
         values.put("CLIENT_ID",ClientID);


         return values;
    }
}
