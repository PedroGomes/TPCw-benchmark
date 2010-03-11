/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package benchmarks.TpcwBenchmark.testEntities;

import benchmarks.interfaces.Entity;
import java.util.Random;
import java.util.TreeMap;

/**
 *
 * @author pedro
 */
public class Address implements Entity{

    String street1;
    String street2;
    String city;
    String state;
    String zip;
    String country;
    String addr_id;

    public Address(String key, String street1, String street2, String city, String state, String zip, String country) {
        
        Random r =  new Random();
        this.street1 = street1;
        this.street2 = street2;
        this.city = city;
        this.state = state;
        this.zip = zip;
        this.country = country;
        this.addr_id = key;
    }


    public String getAddr_id() {
        return addr_id;
    }

    public void setAddr_id(String addr_id) {
        this.addr_id = addr_id;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getStreet1() {
        return street1;
    }

    public void setStreet1(String street1) {
        this.street1 = street1;
    }

    public String getStreet2() {
        return street2;
    }

    public void setStreet2(String street2) {
        this.street2 = street2;
    }

    public String getZip() {
        return zip;
    }

    public void setZip(String zip) {
        this.zip = zip;
    }

    public TreeMap<String, Object> getValuesToInsert() {
         TreeMap<String, Object> values =  new TreeMap<String, Object>();

         values.put("ADDR_STREET1", street1);
         values.put("ADDR_STREET2", street2);
         values.put("ADDR_CITY", city);
         values.put("ADDR_STATE", state);
         values.put("ADDR_ZIP", zip);
         values.put("ADDR_CO_ID", country);


         return values;
    }

    public String getKeyName() {
        return "ADDR_ID";
    }




}
