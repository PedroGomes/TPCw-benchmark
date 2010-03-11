/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package benchmarks.interfaces;

import java.util.TreeMap;

/**
 *
 * @author pedro
 */
public interface Entity {

    public String getKeyName();

    public TreeMap<String, Object> getValuesToInsert();

}
