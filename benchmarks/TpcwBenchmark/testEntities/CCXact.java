/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package benchmarks.TpcwBenchmark.testEntities;

import benchmarks.interfaces.Entity;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.TreeMap;

/**
 *
 * @author pedro
 */
public class CCXact implements Entity{


    long cx_id ;
    String type;
    long num;
    String name;
    java.sql.Date expiry;
    /* String authId;*/
    double total;
    java.sql.Timestamp shipDate;
    long order;
    String country;

    public CCXact(String type, long num, String name, Date expiry, double total, Timestamp shipDate, long order, String country) {

        this.cx_id = order;
        this.type = type;
        this.num = num;
        this.name = name;
        this.expiry = expiry;
        this.total = total;
        this.shipDate = shipDate;
        this.order = order;
        this.country = country;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public Date getExpiry() {
        return expiry;
    }

    public void setExpiry(Date expiry) {
        this.expiry = expiry;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getNum() {
        return num;
    }

    public void setNum(long num) {
        this.num = num;
    }

    public long getOrder() {
        return order;
    }

    public void setOrder(long order) {
        this.order = order;
    }

    public Timestamp getShipDate() {
        return shipDate;
    }

    public void setShipDate(Timestamp shipDate) {
        this.shipDate = shipDate;
    }

    public double getTotal() {
        return total;
    }

    public void setTotal(double total) {
        this.total = total;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public TreeMap<String, Object> getValuesToInsert() {
         TreeMap<String, Object> values =  new TreeMap<String, Object>();

         values.put("CX_TYPE", type);
         values.put("CX_CC_NUM", num);
         values.put("CX_CC_NAME", name);
         values.put("CX_CC_EXPIRY", expiry);
         values.put("CX_XACT_DATE", shipDate);
         values.put("CX_XACT_AMT", total);
         values.put("CX_CO_ID", country);

         return values;
    }

    public String getKeyName() {
         return "CX_O_ID";
    }



}
