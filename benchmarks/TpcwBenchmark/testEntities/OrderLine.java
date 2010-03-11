/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package benchmarks.TpcwBenchmark.testEntities;

import benchmarks.interfaces.Entity;
import java.util.TreeMap;

/**
 * OL_ID
 * OL_O_ID
 * OL_I_ID
 * OL_QTY
 * OL_DISCOUNT
 * OL_COMMENT
 */
public class OrderLine implements Entity{

    String OL_ID;
    long OL_O_ID;
    String OL_I_ID;
    int OL_QTY;
    float OL_DISCOUNT;
    String OL_COMMENT;



    public OrderLine(String OL_ID, long OL_O_ID, String OL_I_ID, int OL_QTY, float OL_DISCOUNT, String OL_COMMENT) {
        this.OL_ID = OL_ID;
        this.OL_O_ID = OL_O_ID;
        this.OL_I_ID = OL_I_ID;
        this.OL_QTY = OL_QTY;
        this.OL_DISCOUNT = OL_DISCOUNT;
        this.OL_COMMENT = OL_COMMENT;
    }

    public TreeMap<String, Object> getValuesToInsert() {
        
        TreeMap<String, Object> values =  new TreeMap<String, Object>();

         values.put("OL_O_ID",OL_O_ID);
         values.put("OL_I_ID",OL_I_ID);
         values.put("OL_QTY", OL_QTY);
         values.put("OL_DISCOUNT", OL_DISCOUNT);
         values.put("OL_COMMENT", OL_COMMENT);
       
         return values;

    }

    public String getKeyName() {
        return "OL_ID";
    }

    

}
