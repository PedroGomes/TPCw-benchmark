package benchmarks.helpers;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;


public class Controler {



    public static void main(String[] args) {



        String host = "localhost";
        for(int i = 0; i<args.length ; i++){
              if(args[i].equalsIgnoreCase("-H")){
                  if(args[i+1]!=null&&!args[i+1].isEmpty())
                  host = args[i+1];
              }
        }
        try {
            Socket s =  new Socket(host,64546);
            PrintWriter writer = new PrintWriter(s.getOutputStream(), true) ;
            writer.write("KILL\n");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

}
