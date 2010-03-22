/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package benchmarks.TpcwBenchmark;

import benchmarks.interfaces.DatabaseBenchmarkInterfaceFactory;

import java.util.Map;

/**
 *
 * @author pedro
 */
public interface TPCWBenchmarkInterface extends DatabaseBenchmarkInterfaceFactory {

    public enum BuyingResult {
        BOUGHT, //Product bought
        NOT_AVAILABLE, //not avilable, the product has no stock, so you cant buy it
        OUT_OF_STOCK, //bought product, but there is no stock to deliver the product
        DOES_NOT_EXIST //debug result, the item does not exist
    };

    interface TPCWBenchmarkInterfaceClient extends BenchmarkInterfaceClient{

       public void searchTop10Books();

       public void addToCart(int Cart, String item_id, int qty );

       public Map<String,Integer> readCart(int Cart);

       public BuyingResult BuyCartItem(String item_id,int qty); 
    }



    
}
