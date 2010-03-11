/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package benchmarks.DatabaseEngineInterfaces;


import benchmarks.TpcwBenchmark.TPCWBenchmarkInterface;
import benchmarks.TpcwBenchmark.testEntities.Item;
import java.util.Map;


/**
 *
 * @author pedro
 */
public class TPCWOperations_Cassandra implements TPCWBenchmarkInterface{

    public void searchTop10Books() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void addToCart(int Cart, String item_id, int qty ) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public BuyingResult BuyCartItem(String item_id, int qty) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Map<String, Integer> readCart(int Cart) {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    

    
}
