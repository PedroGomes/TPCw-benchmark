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

    public void init(DataBaseCRUDInterface databaseInterface, int number_threads, long networkDelay , Map<String,String> info);

    public boolean populate();

    public void cleanDB();

    public void BenchmarkClean();
}
