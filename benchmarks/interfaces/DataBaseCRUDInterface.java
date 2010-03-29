/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package benchmarks.interfaces;

import java.util.List;
import java.util.Map;

/**
 * @author pedro
 */
public interface DataBaseCRUDInterface {

    public static final String OBJECT ="OBJ";
    public static final String TIME ="TI";
    public static final String TIME_TYPE ="TTY";

    public CRUDclient getClient();

    public void simulatedDelay(long delay); 

    interface CRUDclient {

        public Object insert(String key, String path, Entity value);

        public void remove(String key, String path, String column);

        public void update(String key, String path, String column, Object value);

        public Object read(String key, String path, String column);

        public Map<String,Map<String,Object>> rangeQuery(String table,List<String> fields ,int limit);

        public void truncate(String path);

        public void closeClient();

    }

}

