/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package benchmarks.helpers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.GregorianCalendar;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.math.JVMRandom;

/**
 *
 * @author pedro
 */
public class BenchmarkUtil {

    
        private static Random rand = new Random();


 public static GregorianCalendar getRandomDate(int firstYar, int lastYear) {
        int month, day, year, maxday;

        year = getRandomInt(firstYar, lastYear);
        month = getRandomInt(0, 11);

        maxday = 31;
        if (month == 3 | month == 5 | month == 8 | month == 10) {
            maxday = 30;
        } else if (month == 1) {
            maxday = 28;
        }

        day = getRandomInt(1, maxday);
        return new GregorianCalendar(year, month, day);
    }

   public  static int getRandomInt(int lower, int upper) {

        int num = (int) Math.floor(rand.nextDouble() * ((upper + 1) - lower));
        if (num + lower > upper || num + lower < lower) {
            System.out.println("ERROR: Random returned value of of range!");
            System.exit(1);
        }
        return num + lower;
    }

    public  static int getRandomNString(int num_digits) {
        int return_num = 0;
        for (int i = 0; i < num_digits; i++) {
            return_num += getRandomInt(0, 9)
                    * (int) java.lang.Math.pow(10.0, (double) i);
        }
        return return_num;
    }

   public  static String getRandomAString(int min, int max) {
        String newstring = new String();
        int i;
        final char[] chars = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k',
            'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
            'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
            'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
            'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '!', '@', '#',
            '$', '%', '^', '&', '*', '(', ')', '_', '-', '=', '+',
            '{', '}', '[', ']', '|', ':', ';', ',', '.', '?', '/',
            '~', ' '}; //79 characters
        int strlen = (int) Math.floor(rand.nextDouble() * ((max - min) + 1));
        strlen += min;
        for (i = 0; i < strlen; i++) {
            char c = chars[(int) Math.floor(rand.nextDouble() * 79)];
            newstring = newstring.concat(String.valueOf(c));
        }
        return newstring;
    }


       public static byte[] getBytes(Object obj) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            oos.close();
            bos.close();
            byte[] data = bos.toByteArray();
            return data;
        } catch (IOException ex) {
            Logger.getLogger(BenchmarkUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static Object toObject(byte[] bytes) {
        try {
            Object object = null;
            object = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes)).readObject();
            return object;
        } catch (IOException ex) {
            Logger.getLogger(BenchmarkUtil.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(BenchmarkUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

 public static String getRandomAString(int length) {
        String newstring = new String();
        int i;
        final char[] chars = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k',
            'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
            'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
            'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
            'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '!', '@', '#',
            '$', '%', '^', '&', '*', '(', ')', '_', '-', '=', '+',
            '{', '}', '[', ']', '|', ':', ';', ',', '.', '?', '/',
            '~', ' '}; //79 characters
        for (i = 0; i < length; i++) {
            char c = chars[(int) Math.floor(rand.nextDouble() * 79)];
            newstring = newstring.concat(String.valueOf(c));
        }
        return newstring;
    }
    

}