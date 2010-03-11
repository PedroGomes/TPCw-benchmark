/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package benchmarks.TpcwBenchmark.testEntities;

import benchmarks.interfaces.Entity;
import java.sql.Timestamp;
import java.util.GregorianCalendar;
import java.util.TreeMap;

/**
O_ID
O_C_ID
O_DATE
O_SUB_TOTAL
O_TAX
O_TOTAL
O_SHIP_TYPE
O_SHIP_DATE
O_BILL_ADDR_ID
O_SHIP_ADDR_ID
O_STATUS
 */
public class Orders implements Entity {

    long O_ID;
    String O_C_ID; //  Customer customer?
    java.sql.Timestamp O_DATE;
    float O_SUB_TOTAL;
    float O_TAX;
    float O_TOTAL;
    String O_SHIP_TYPE;
    java.sql.Timestamp O_SHIP_DATE;
    String O_STATUS;
    String billAddress;
    String O_SHIP_ADDR;

    public Orders(long O_ID, String O_C_ID, Timestamp O_DATE, float O_SUB_TOTAL, float O_TAX, float O_TOTAL, String O_SHIP_TYPE, Timestamp shipDate, String O_STATUS, String billAddress, String O_SHIP_ADDR) {
        this.O_ID = O_ID;
        this.O_C_ID = O_C_ID;
        this.O_DATE = O_DATE;
        this.O_SUB_TOTAL = O_SUB_TOTAL;
        this.O_TAX = O_TAX;
        this.O_TOTAL = O_TOTAL;
        this.O_SHIP_TYPE = O_SHIP_TYPE;
        this.O_SHIP_DATE = shipDate;
        this.O_STATUS = O_STATUS;
        this.billAddress = billAddress;
        this.O_SHIP_ADDR = O_SHIP_ADDR;
    }

    public String getO_C_ID() {
        return O_C_ID;
    }

    public void setO_C_ID(String O_C_ID) {
        this.O_C_ID = O_C_ID;
    }

    public Timestamp getO_DATE() {
        return O_DATE;
    }

    public void setO_DATE(Timestamp O_DATE) {
        this.O_DATE = O_DATE;
    }

    public long getO_ID() {
        return O_ID;
    }

    public void setO_ID(long O_ID) {
        this.O_ID = O_ID;
    }

    public String getO_SHIP_ADDR() {
        return O_SHIP_ADDR;
    }

    public void setO_SHIP_ADDR(String O_SHIP_ADDR) {
        this.O_SHIP_ADDR = O_SHIP_ADDR;
    }

    public String getO_SHIP_TYPE() {
        return O_SHIP_TYPE;
    }

    public void setO_SHIP_TYPE(String O_SHIP_TYPE) {
        this.O_SHIP_TYPE = O_SHIP_TYPE;
    }

    public String getO_STATUS() {
        return O_STATUS;
    }

    public void setO_STATUS(String O_STATUS) {
        this.O_STATUS = O_STATUS;
    }

    public float getO_SUB_TOTAL() {
        return O_SUB_TOTAL;
    }

    public void setO_SUB_TOTAL(float O_SUB_TOTAL) {
        this.O_SUB_TOTAL = O_SUB_TOTAL;
    }

    public float getO_TAX() {
        return O_TAX;
    }

    public void setO_TAX(float O_TAX) {
        this.O_TAX = O_TAX;
    }

    public float getO_TOTAL() {
        return O_TOTAL;
    }

    public void setO_TOTAL(float O_TOTAL) {
        this.O_TOTAL = O_TOTAL;
    }

    public String getBillAddress() {
        return billAddress;
    }

    public void setBillAddress(String billAddress) {
        this.billAddress = billAddress;
    }

    public Timestamp getShipDate() {
        return O_SHIP_DATE;
    }

    public void setShipDate(Timestamp shipDate) {
        this.O_SHIP_DATE = shipDate;
    }

    public TreeMap<String, Object> getValuesToInsert() {

//        O_ID
//O_C_ID
//O_DATE
//O_SUB_TOTAL
//O_TAX
//O_TOTAL
//O_SHIP_TYPE
//O_SHIP_DATE
//O_BILL_ADDR_ID
//O_SHIP_ADDR_ID
//O_STATUS
//

        TreeMap<String, Object> values = new TreeMap<String, Object>();

        values.put("O_C_ID", O_C_ID);
        values.put("O_DATE", O_DATE);
        values.put("O_SUB_TOTAL", O_SUB_TOTAL);
        values.put("O_TAX", O_TAX);
        values.put("O_TOTAL", O_TOTAL);

        values.put("O_SHIP_TYPE", O_SHIP_TYPE);
        values.put("O_SHIP_DATE", O_SHIP_DATE);

        values.put("O_BILL_ADDR_ID", billAddress);
        values.put("O_SHIP_ADDR_ID", O_SHIP_ADDR);

        values.put("O_STATUS", O_STATUS);


        return values;
    }

    public String getKeyName() {
        return "O_ID";
    }
}
