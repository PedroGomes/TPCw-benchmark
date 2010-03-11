/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package benchmarks.interfaces;

/**
 *
 * @author pedro
 */
public interface CRUD {

        public void insert(String key , String path, Entity value);

        public void remove(String key , String path);

        public void update(String key , String path, String column , Object value);

        public Object read(String key , String path, String column);

        public void truncate(String path, int number_keys);
}
