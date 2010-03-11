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
public class ShoppingCartLine implements Entity{


    int ShoppingCartID;
    Item book;
    ShoppingCart cart;
    int qty;

    public ShoppingCartLine(int ShoppingCartID, Item book, ShoppingCart cart, int qty) {
        this.ShoppingCartID = ShoppingCartID;
        this.book = book;
        this.cart = cart;
        this.qty = qty;
    }


    public ShoppingCart getCart() {
        return cart;
    }

    public void setCart(ShoppingCart cart) {
        this.cart = cart;
    }


    public Item getBook() {
        return book;
    }

    public void setBook(Item book) {
        this.book = book;
    }

    public int getQty() {
        return qty;
    }

    public void setQty(int qty) {
        this.qty = qty;
    }

    public TreeMap<String, Object> getValuesToInsert() {
              TreeMap<String, Object> values =  new TreeMap<String, Object>();

         values.put("QTY",qty);
         values.put("KEY_BOOK",book.getI_id());
         values.put("KEY_SHOPPING_CART",cart.i_id);

         return values;
    }

    public String getKeyName() {
        return "KEY_SHOPPING_CART";
    }


}
