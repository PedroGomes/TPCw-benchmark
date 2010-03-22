/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package benchmarks.interfaces;

import java.util.Map;

/**
 *
 * @author pedro
 */
public  interface BenchmarkPopulator {

    /**
     *
     * @return true if sucess, false otherwise
     */
    public boolean populate();

    public void cleanDB();

    public  Map<String,Object> getUseFullData();

}
