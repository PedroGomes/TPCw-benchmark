/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package benchmarks.dataStatistics;

import benchmarks.DatabaseEngineInterfaces.CassandraInterface;
import benchmarks.helpers.Pair;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author pedro
 */
public class ResultHandler {

    private int testsamples = 100;
    String test_name;                         //pair(time,result)
    private HashMap<String, ArrayList<Pair<Long, Long>>> results;

    private HashMap<String, HashMap<String, ArrayList<Object>>> data;
    private TreeMap<String, ArrayList<String>> dataHeader;

    private HashMap<String, HashMap<String, Long>> events;

    /**
     * @param run_data_divisions the division in the data by runs. If -1 all will be inserted un the same run;
     */
    public ResultHandler(String name, int run_data_divisions) {
        this.test_name = name;
        testsamples = run_data_divisions;
        results = new HashMap<String, ArrayList<Pair<Long, Long>>>();
        events = new HashMap<String, HashMap<String, Long>>();
        data = new HashMap<String, HashMap<String, ArrayList<Object>>>();
        dataHeader = new TreeMap<String, ArrayList<String>>();
    }

    /**
     * **LOG OPERATIONS****
     */

    public void logResult(String operation, long result) {

        Pair<Long, Long> resultPair = new Pair<Long, Long>(System.currentTimeMillis(), result);
        if (!results.containsKey(operation)) {
            results.put(operation, new ArrayList<Pair<Long, Long>>());
        }
        results.get(operation).add(resultPair);
    }

    public void countEvent(String eventType, String event, long number) {

        if (!events.containsKey(eventType)) {
            HashMap<String, Long> new_events = new HashMap<String, Long>();
            new_events.put(event, number);
            events.put(eventType, new_events);
        } else {
            if (!events.get(eventType).containsKey(event)) {
                events.get(eventType).put(event, number);
            } else {
                long count = events.get(eventType).get(event) + number;
                events.get(eventType).put(event, count);
            }
        }

    }

    public void recordData(String eventType, String event, List<Object> record_data) {

        if (!data.containsKey(eventType)) {
            HashMap<String, ArrayList<Object>> data_slot = new HashMap<String, ArrayList<Object>>();
            ArrayList<Object> data_list = new ArrayList<Object>(record_data);
            data_slot.put(event, data_list);
            data.put(eventType, data_slot);
        } else {
            ArrayList<Object> data_list = new ArrayList<Object>(record_data);
            data.get(eventType).put(event, data_list);
        }

    }

    /**
     * UTILITIES***
     */

    public void cleanResults() {
        results.clear();
        data.clear();
        events.clear();
        System.gc();

    }

    public void setDataHeader(String EventType, ArrayList<String> dataHeader) {
        this.dataHeader.put(EventType, dataHeader);

    }

    public void addResults(ResultHandler other_results) {

        Map<String, ArrayList<Pair<Long, Long>>> new_results = other_results.results;

        for (String event_name : new_results.keySet()) {
            if (!this.results.containsKey(event_name)) {
                this.results.put(event_name, new_results.get(event_name));
            } else {
                for (Pair<Long, Long> l : new_results.get(event_name)) {
                    this.results.get(event_name).add(l);
                }
            }
        }

        Map<String, HashMap<String, Long>> new_events = other_results.events;

        for (String event_name : new_events.keySet()) {
            if (!this.events.containsKey(event_name)) {
                this.events.put(event_name, new_events.get(event_name));
            } else {
                HashMap<String, Long> new_event_count = new_events.get(event_name);
                HashMap<String, Long> this_event_count = this.events.get(event_name);
                for (String event_count_name : new_event_count.keySet()) {
                    if (this_event_count.containsKey(event_count_name)) {
                        this_event_count.put(event_count_name, this_event_count.get(event_count_name) + new_event_count.get(event_count_name));
                    } else {
                        this_event_count.put(event_count_name, new_event_count.get(event_count_name));
                    }
                }
            }
        }
    }


    /**
     * OUTPUP***
     */


    public void listDataToSOutput() {

        System.out.println("\n\n------- RESULTS FOR: " + test_name + "-------");
        System.out.println("--runs: " + testsamples);
        for (String dataOperation : results.keySet()) {
            System.out.println("OPERATION: " + dataOperation);
            ArrayList<Pair<Long, Long>> result_data = results.get(dataOperation);
            boolean do_multipleruns = testsamples < 0 ? false : true;


            int total_amount = 0;
            int currrent_amount = 0;
            int current_run = 0;
            int run = 0;
            ArrayList<Long> run_result = new ArrayList<Long>();
            for (Pair<Long, Long> res : result_data) {

                run_result.add(res.right);
                total_amount += res.right;
                currrent_amount += res.right;
                current_run += 1;

                if (do_multipleruns && current_run == testsamples) {
                    System.out.println("--RESULTS FOR RUN " + run + "");
                    double average = (currrent_amount * 1.0d) / (testsamples * 1.0d);
                    System.out.println("Average: " + average);
                    double variance = 0.0;
                    long aux = 0;
                    for (Long run_res : run_result) {
                        aux += Math.pow((run_res - average), 2);
                    }
                    variance = aux * (1d / (run_result.size() - 1d));
                    System.out.println("Variance: " + variance);

                    run++;
                    currrent_amount = 0;
                    current_run = 0;


                    run_result = new ArrayList<Long>();
                }
            }
            if (!result_data.isEmpty()) {

                System.out.println("----TOTAL RESULTS:----");
                double average = (total_amount * 1.0d) / (result_data.size() * 1.0d);
                System.out.println("Average: " + average);
                double variance = 0.0;
                long aux = 0;
                for (Pair<Long, Long> run_res : result_data) {
                    aux += Math.pow((run_res.right - average), 2);
                }
                variance = aux * (1d / (result_data.size() - 1d));
                System.out.println("Variance: " + variance + "\n\n");
            }
        }
        if (!events.isEmpty()) {
            System.out.println("****EVENT COUNT****");
            for (String eventType : events.keySet()) {
                System.out.println("+EVENT TYPE: " + eventType);
                for (String event : events.get(eventType).keySet()) {
                    System.out.println("\t>>" + event + " : " + events.get(eventType).get(event));
                }
            }

        }
        if (!data.isEmpty()) {
            System.out.println("\n\n***DATA RECORDS ARE NOT SHOWN IN THIS METHOD - USE SAVE TO FILE OPTIONS****\n");
        }


    }

    public void listDataToFile(String filename) {


    }

    public void listDataToFile(File filename) {
    }

    public void listDatatoFiles(String folder_name, String perfix, boolean doMultiple) {


        System.out.println("\n\n-------WRITING RESULTS FOR: " + test_name + "-------");
        File enclosing_folder = new File(folder_name);
        if (!enclosing_folder.isDirectory()) {
            System.out.println("NOT A FOLDER: ENCLOSING FOLDER USED");
            enclosing_folder = enclosing_folder.getParentFile();
        }

        GregorianCalendar date = new GregorianCalendar();
        String suffix = date.get(GregorianCalendar.YEAR) + "_" + date.get(GregorianCalendar.MONTH) + "_" + date.get(GregorianCalendar.DAY_OF_MONTH) + "_" + date.get(GregorianCalendar.HOUR_OF_DAY) + "_" + date.get(GregorianCalendar.MINUTE) + "";

        File folder = new File(enclosing_folder.getAbsolutePath() + "/" + test_name + suffix);

        if (!folder.exists()) {
            boolean created = folder.mkdir();
            if (!created) {
                System.out.println("RESULT FOLDER DOES NOT EXISTS AND CANT BE CREATED");
                return;
            }
        }
        System.out.println("OUTPUT FOLDER: " + folder.getName());
        resultComparator comparator = new resultComparator();
        for (String dataOperation : results.keySet()) {

            System.out.println("OPERATION: " + dataOperation);
            ArrayList<Pair<Long, Long>> result_data = results.get(dataOperation);
            boolean do_multipleruns = (testsamples < 0 && doMultiple) ? false : true;


            int current_run = 0;
            int run = 0;
            File operation_results_file = new File(folder.getPath() + "/" + dataOperation);

            FileOutputStream out = null;
            BufferedOutputStream stream = null;
            try {
                out = new FileOutputStream(operation_results_file);
                stream = new BufferedOutputStream(out);

            } catch (FileNotFoundException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }


            try {
                if (!do_multipleruns) {
                    stream.write(("results , time  \n").getBytes());
                } else {
                    stream.write(("results , time , run\n").getBytes());

                }

            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }


            int length = result_data.size();
            for (int z = 0; z < length; z++) {

                Pair<Long, Long> res = result_data.get(z);

                current_run += 1;

                String result_line = res.right + " , " + res.left + "";
                if (do_multipleruns) {
                    result_line = result_line + " , " + run;
                }
                result_line = result_line + "\n";

                if (do_multipleruns && current_run == testsamples) {
                    run++;
                }
                try {
                    stream.write(result_line.getBytes());

                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }

            try {
                stream.flush();
                stream.close();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } finally {
                try {
                    out.close();
                } catch (IOException ex) {
                    Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
                }
            }


        }
        if (!events.isEmpty()) {
            System.out.println("****WRITING EVENT COUNT****");


            for (String eventType : events.keySet()) {
                File event_results_file = new File(folder.getPath() + "/" + eventType);
                FileOutputStream out = null;
                BufferedOutputStream stream = null;
                try {
                    out = new FileOutputStream(event_results_file);
                    stream = new BufferedOutputStream(out);

                } catch (FileNotFoundException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }


                System.out.println("+EVENT TYPE: " + eventType);
                for (String event : events.get(eventType).keySet()) {
                    // System.out.println("\t>>" + event + " : " + events.get(eventType).get(event));
                    try {
                        out.write((event + " , " + events.get(eventType).get(event) + "\n").getBytes());
                    } catch (IOException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                }


                try {
                    stream.flush();
                    stream.close();
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                } finally {
                    try {
                        out.close();
                    } catch (IOException ex) {
                        Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
                    }


                }

            }

        }


        if (!data.isEmpty()) {
            System.out.println("****WRITING DATA COUNT****");


            for (String eventType : data.keySet()) {
                File event_results_file = new File(folder.getPath() + "/" + eventType);
                FileOutputStream out = null;
                BufferedOutputStream stream = null;
                try {
                    out = new FileOutputStream(event_results_file);
                    stream = new BufferedOutputStream(out);

                } catch (FileNotFoundException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }


                System.out.println("+DATA EVENT TYPE: " + eventType);
                int i = 0;
                try {

                    if(dataHeader.get(eventType)==null){
                        dataHeader.put(eventType,new ArrayList<String>());
                    }

                    for (String header_name : dataHeader.get(eventType)) {
                        if (i != 0)
                            out.write(" , ".getBytes());

                        out.write(header_name.getBytes());
                        i++;
                    }
                    out.write("\n".getBytes());
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }


                for (String event : data.get(eventType).keySet()) {
                    try {
                        out.write((event).getBytes());
                        for (Object o : data.get(eventType).get(event)) {
                            out.write((" , " + o.toString()).getBytes());
                        }
                        out.write("\n".getBytes());

                    } catch (IOException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                }


                try {
                    stream.flush();
                    stream.close();
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                } finally {
                    try {
                        out.close();
                    } catch (IOException ex) {
                        Logger.getLogger(CassandraInterface.class.getName()).log(Level.SEVERE, null, ex);
                    }


                }

            }

        }

    }

    public void doRstatistcs(String filePerfix) {


    }


    class resultComparator implements Comparator {

        public int compare(Object o1, Object o2) {

            if (!(o1 instanceof Pair) || !(o2 instanceof Pair))
                return 0;

            Pair<Long, Long> p1 = (Pair<Long, Long>) o1;
            Pair<Long, Long> p2 = (Pair<Long, Long>) o2;

            if (p1.left > p2.left)

                return 1;

            else if (p1.left < p2.left)

                return -1;

            else

                return 0;
        }
    }


}

