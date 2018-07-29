import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by bishnu.agrawal on 29/7/18.
 */
public class WordCountMain {
    public static final int NO_OF_THREADS = 4;
    public static final int MAX_LINES_IN_MEMORY = 20000;
    public static Map<String, Integer> wc = new HashMap<>();
    public static BlockingQueue<String> queue = new ArrayBlockingQueue<String>(MAX_LINES_IN_MEMORY);
    public static final Object lock = new Object();
    public static CountDownLatch latch = new CountDownLatch(NO_OF_THREADS);
    public static boolean stopProcessing = false;
    private static int lineProcessed = 0;

    public static void main(String[] args) throws InterruptedException {
        ExecutorService executorService = null;
        Thread.sleep(10);
        try {
            if(args.length==0){
                System.out.println("Input file expected; exiting");
                System.exit(0);
            }
            String filePath = args[0];
            executorService = Executors.newFixedThreadPool(NO_OF_THREADS);
            for (int i = 0; i < NO_OF_THREADS; i++) {
                executorService.submit(new WordCountTask());
            }
            readFile(filePath);
            stopProcessing = true;
            latch.await();
            printWordFrequency();
        } finally {
            executorService.shutdown();
        }
    }

    private static void printWordFrequency() {
        for (Map.Entry<String, Integer> entry : wc.entrySet()) {
            System.out.println("Word: "+entry.getKey()+" Value: "+entry.getValue());
        }
    }

    private static void readFile(String filePath){
        try {
            BufferedReader reader = new BufferedReader(new FileReader("/home/likewise-open/UGAM/bishnu.agrawal/Desktop/input.txt"));
            String line = reader.readLine();
            lineProcessed++;
            while (line!=null){
                queue.put(line);
                lineProcessed++;
                if(lineProcessed%1000==0){
                    System.out.println("Total line pushed to queue: "+lineProcessed);
                }
                line = reader.readLine();
            }
            System.out.println("Total line pushed to queue: "+lineProcessed);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
