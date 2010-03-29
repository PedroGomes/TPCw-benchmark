package benchmarks.helpers;

import benchmarks.dataStatistics.ConcurrentResultHandler;

import java.io.Console;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by IntelliJ IDEA.
 * User: pedro
 * Date: Mar 26, 2010
 * Time: 9:25:16 PM
 * To change this template use File | Settings | File Templates.
 */
public class ProgressBar {

    AtomicInteger[] progress;
    int top_limit;
    boolean[] terminated;
    int terminated_count;
    int size;
    boolean perc_term = true;

    public ProgressBar(int numberElements, int limit) {
        progress = new AtomicInteger[numberElements];
        terminated = new boolean[numberElements];
        for (int i = 0; i < numberElements; i++) {
            progress[i] = new AtomicInteger(0);
            terminated[i] = false;
        }
        top_limit = limit;
        size = numberElements;


        terminated_count = 0;

    }

    public void setProgress(int index, int progress) {
        this.progress[index].set(progress);
    }

    public void addProgress(int index, int progress) {
        this.progress[index].addAndGet(progress);
    }

    public void increment(int index) {
        this.progress[index].incrementAndGet();
    }

    public void printProcess(final long delays) {

        Runnable run = new Runnable() {

            public void run() {
                boolean terminate = false;

                while (!terminate) {

                    int total = 0;

                    for (int i = 0; i < size; i++) {
                        int pg = progress[i].get();
                        if (!terminated[i] && pg == top_limit) {
                            terminated[i] = true;
                            terminated_count++;
                        }
                        total += pg;
                    }
                    int vintage = (total * 20) / (top_limit * size);
                    int percentage = (total * 100) / (top_limit * size);
                    int rest = 20 - vintage;

                    System.out.print("\r");
                    System.out.print("||");
                    for (int pc = 0; pc < vintage; pc++) {
                        System.out.print("=");
                    }
                    for (int pc = 0; pc < rest; pc++) {
                        System.out.print("*");
                    }
                    System.out.print("||");
                    if (perc_term) {
                        System.out.print("[" + percentage + "%]");
                    } else {
                        System.out.print("(" + terminated_count + "/" + size + ")");
                    }

                    perc_term = !perc_term;


                    try {
                        Thread.sleep(delays);
                    } catch (InterruptedException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                    if (size == terminated_count) {
                        terminate = true;
                        System.out.println();
                    }

                }
            }
        };
        Thread t = new Thread(run);
        t.start();
    }

    public static void main(String[] args) {
        ProgressBar bar = new ProgressBar(2, 10);
        bar.printProcess(10);

        Runnable run = new Runnable() {
            ProgressBar pbar;

            public Runnable setBar(ProgressBar bar) {
                pbar = bar;
                return this;
            }

            public void run() {
                for (int i = 0; i < 10; i++) {
                    pbar.increment(0);
                    try {
                        Thread.sleep(15);
                    } catch (InterruptedException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                }
            }
        }.setBar(bar);

        Runnable run2 = new Runnable() {
            ProgressBar pbar;

            public Runnable setBar(ProgressBar bar) {
                pbar = bar;
                return this;
            }

            public void run() {
                for (int i = 0; i < 10; i++) {
                    pbar.increment(1);
                    try {
                        Thread.sleep(15);
                    } catch (InterruptedException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                }

            }
        }.setBar(bar);

        Thread t1 = new Thread(run);
        Thread t2 = new Thread(run2);
        t1.start();
        t2.start();


    }
}
