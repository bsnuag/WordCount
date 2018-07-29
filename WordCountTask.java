import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Created by UGAM\bishnu.agrawal on 29/7/18.
 */
public class WordCountTask implements Runnable{
    private Map<String, Integer> wcMap = new HashMap<>();
    private int lineProcessed = 0;
    public void run() {
        try {
            while (true) {
                String line = WordCountMain.queue.poll();
                if(line!=null) {
                    lineProcessed++;
                    String[] split = line.split("\\s+");
                    updateWordCount(split);
                } else if(line==null && WordCountMain.stopProcessing){
                    break;
                }
            }
        } finally {
            updateWordCount();
            printStat();
            WordCountMain.latch.countDown();
        }
    }

    private void printStat() {
        System.out.println("Thread: "+Thread.currentThread().getName()+" processed: "+lineProcessed+" lines");
    }

    private void updateWordCount() {
        synchronized (WordCountMain.lock){
            for (Map.Entry<String, Integer> entry : wcMap.entrySet()) {
                WordCountMain.wc.merge(entry.getKey(),entry.getValue(),(a,b)->a+b);
            }
        }
    }

    private void updateWordCount(String[] split) {
        for (String word : split) {
            wcMap.merge(word,1,(a,b)->a+b);
        }
    }
}
