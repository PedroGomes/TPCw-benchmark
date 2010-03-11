/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package benchmarks.TpcwBenchmark.testEntities;

import benchmarks.interfaces.Entity;
import java.util.TreeMap;

/**
 *
 * @author pedro
 */
public class ShoppingCart implements Entity{

    int i_id;

    public ShoppingCart(int i_id) {
        this.i_id = i_id;
    }

    public int getI_id() {
        return i_id;
    }

    public void setI_id(int i_id) {
        this.i_id = i_id;
    }

    public TreeMap<String, Object> getValuesToInsert() {
         TreeMap<String, Object> values =  new TreeMap<String, Object>();
         return values;
    }

    public String getKeyName() {
        return "SC_ID";
    }



}
