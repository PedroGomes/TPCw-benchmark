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
public class Country implements Entity{

    String name;
    double currency;
    String exchange;
    String co_id;

    public Country(String co_id,String name, double currency, String exchange) {
        this.name = name;
        this.currency = currency;
        this.exchange = exchange;
        this.co_id =co_id;
    }


    public String getCo_id() {
        return name;
    }


    public double getCurrency() {
        return currency;
    }

    public void setCurrency(double currency) {
        this.currency = currency;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TreeMap<String, Object> getValuesToInsert() {
         TreeMap<String, Object> values =  new TreeMap<String, Object>();

         values.put("CO_NAME", name);
         values.put("CO_CURRENCY", currency);
         values.put("CO_EXCHANGE", exchange) ;


         return values;
    }

    public String getKeyName() {
        return "CO_ID";
    }

}
