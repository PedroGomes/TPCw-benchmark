/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package benchmarks.TpcwBenchmark.testEntities;

import benchmarks.interfaces.Entity;
import java.sql.Date;
import java.util.List;
import java.util.TreeMap;

/**
 *
 * @author pedro
 */
/**
I_ID
I_TITLE
I_A_ID
I_PUB_DATE X
I_PUBLISHER
I_SUBJECT 
I_DESC
I_RELATED[1 -5]
I_THUMBNAIL X
I_IMAGE X
I_SRP X
I_COST
I_AVAIL
I_STOCK
I_ISBN
I_PAGE
I_BACKING
I_DIMENSION
 **/
public class Item implements Entity{

    String i_id;
    String I_TITLE; //ID
    java.sql.Date pubDate;
    String I_AUTHOR;
    String I_PUBLISHER;
    String I_DESC;
    String I_SUBJECT;
    String thumbnail;
    String image;
    float I_COST;
    long I_STOCK;
    String isbn;//international id
    double srp;//Suggested Retail Price
    List<String> I_RELATED;
    int I_PAGE;
    java.sql.Date avail; //Data when available
    String I_BACKING;
    String dimensions;
    String author;



    public Item(String i_id, String I_TITLE, Date pubDate, String I_PUBLISHER, String I_DESC, String I_SUBJECT, String thumbnail, String image, float I_COST, long I_STOCK, String isbn, double srp, List<String> I_RELATED, int I_PAGE, Date avail, String I_BACKING, String dimensions, String author) {
        this.i_id = i_id;
        this.I_TITLE = I_TITLE;
        this.pubDate = pubDate;
        this.I_AUTHOR = author;
        this.I_PUBLISHER = I_PUBLISHER;
        this.I_DESC = I_DESC;
        this.I_SUBJECT = I_SUBJECT;
        this.thumbnail = thumbnail;
        this.image = image;
        this.I_COST = I_COST;
        this.I_STOCK = I_STOCK;
        this.isbn = isbn;
        this.srp = srp;
        this.I_RELATED = I_RELATED;
        this.I_PAGE = I_PAGE;
        this.avail = avail;
        this.I_BACKING = I_BACKING;
        this.dimensions = dimensions;
        this.author = author;
    }

    public String getI_AUTHOR() {
        return I_AUTHOR;
    }

    public void setI_AUTHOR(String I_AUTHOR) {
        this.I_AUTHOR = I_AUTHOR;
    }

    public String getI_BACKING() {
        return I_BACKING;
    }

    public void setI_BACKING(String I_BACKING) {
        this.I_BACKING = I_BACKING;
    }

    public float getI_COST() {
        return I_COST;
    }

    public void setI_COST(float I_COST) {
        this.I_COST = I_COST;
    }

    public String getI_DESC() {
        return I_DESC;
    }

    public void setI_DESC(String I_DESC) {
        this.I_DESC = I_DESC;
    }

    public int getI_PAGE() {
        return I_PAGE;
    }

    public void setI_PAGE(int I_PAGE) {
        this.I_PAGE = I_PAGE;
    }

    public String getI_PUBLISHER() {
        return I_PUBLISHER;
    }

    public void setI_PUBLISHER(String I_PUBLISHER) {
        this.I_PUBLISHER = I_PUBLISHER;
    }

    public List<String> getI_RELATED() {
        return I_RELATED;
    }

    public void setI_RELATED(List<String> I_RELATED) {
        this.I_RELATED = I_RELATED;
    }

    public long getI_STOCK() {
        return I_STOCK;
    }

    public void setI_STOCK(long I_STOCK) {
        this.I_STOCK = I_STOCK;
    }

    public String getI_SUBJECT() {
        return I_SUBJECT;
    }

    public void setI_SUBJECT(String I_SUBJECT) {
        this.I_SUBJECT = I_SUBJECT;
    }

    public String getI_TITLE() {
        return I_TITLE;
    }

    public void setI_TITLE(String I_TITLE) {
        this.I_TITLE = I_TITLE;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public Date getAvail() {
        return avail;
    }

    public void setAvail(Date avail) {
        this.avail = avail;
    }

    public String getDimensions() {
        return dimensions;
    }

    public void setDimensions(String dimensions) {
        this.dimensions = dimensions;
    }

    public String getI_id() {
        return i_id;
    }

    public void setI_id(String i_id) {
        this.i_id = i_id;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getIsbn() {
        return isbn;
    }

    public void setIsbn(String isbn) {
        this.isbn = isbn;
    }

    public Date getPubDate() {
        return pubDate;
    }

    public void setPubDate(Date pubDate) {
        this.pubDate = pubDate;
    }

    public double getSrp() {
        return srp;
    }

    public void setSrp(double srp) {
        this.srp = srp;
    }

    public String getThumbnail() {
        return thumbnail;
    }

    public void setThumbnail(String thumbnail) {
        this.thumbnail = thumbnail;
    }

    public TreeMap<String, Object> getValuesToInsert() {
//
//
//        I_ID
//I_TITLE
//I_A_ID
//I_PUB_DATE
//I_PUBLISHER
//I_SUBJECT
//I_DESC
//I_RELATED[1-5]
//I_THUMBNAIL
//I_IMAGE
//I_SRP
//I_COST
//I_AVAIL
//I_STOCK
//I_ISBN
//I_PAGE
//I_BACKING
//I_DIMENSION


        TreeMap<String, Object> values =  new TreeMap<String, Object>();

         values.put("I_TITLE", I_TITLE);
         values.put("I_A_ID",author);
         values.put("I_PUB_DATE", pubDate);
         values.put("I_PUBLISHER", I_PUBLISHER);
         values.put("I_SUBJECT", I_SUBJECT);
         values.put("I_DESC",I_DESC);
         values.put("I_RELATED",I_RELATED);
         values.put("I_THUMBNAIL",thumbnail);
         values.put("I_IMAGE", image);
         values.put("I_SRP", srp);
         values.put("I_COST", I_COST);
         values.put("I_AVAIL", avail);
         values.put("I_STOCK", I_STOCK);
         values.put("I_ISBN",isbn);
         values.put("I_PAGE",I_PAGE);
         values.put("I_BACKING",I_BACKING);
         values.put("I_DIMENSION",dimensions);

         return values;
    }

    public String getKeyName() {
        return "I_ID";
    }


}
