/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package benchmarks.TpcwBenchmark.testEntities;

import benchmarks.interfaces.Entity;
import java.sql.Date;
import java.util.TreeMap;

/**
 *
 * @author pedro
 */
public class Author implements Entity{

    String fname;
    String lname;
    String mname;
    java.sql.Date dob;
    String bio;
    String a_id;

    public Author(String a_id, String fname, String lname, String mname, Date dob, String bio) {
        this.fname = fname;
        this.lname = lname;
        this.mname = mname;
        this.dob = dob;
        this.bio = bio;
        this.a_id = a_id;
    }

    public String getA_id() {
        return a_id;
    }

    public void setA_id(String a_id) {
        this.a_id = a_id;
    }

    public String getBio() {
        return bio;
    }

    public void setBio(String bio) {
        this.bio = bio;
    }

    public Date getDob() {
        return dob;
    }

    public void setDob(Date dob) {
        this.dob = dob;
    }

    public String getFname() {
        return fname;
    }

    public void setFname(String fname) {
        this.fname = fname;
    }

    public String getLname() {
        return lname;
    }

    public void setLname(String lname) {
        this.lname = lname;
    }

    public String getMname() {
        return mname;
    }

    public void setMname(String mname) {
        this.mname = mname;
    }

    public TreeMap<String, Object> getValuesToInsert() {
         TreeMap<String, Object> values =  new TreeMap<String, Object>();

         values.put("A_FNAME", fname);
         values.put("A_LNAME", lname);
         values.put("A_MNAME", mname);
         values.put("A_DOB", dob);
         values.put("A_BIO", bio);

         return values;
    }

    public String getKeyName() {
       return "A_ID";
    }

}
