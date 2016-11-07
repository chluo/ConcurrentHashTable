/**
 * Created by Liheng on 11/7/16.
 */
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class newMain {
    final static int keys_num = 1000000;

    // addthread
    public static class putThread extends Thread{
        int times;
        Map<Integer, Integer> hm;

        putThread(int _times, Map<Integer, Integer> _hm){
            this.times = _times;
            this.hm = _hm;
        }

        public void run(){
            for(int i = 0; i < times; i++){
                // System.out.println("a " + hs.add(i));
                hm.put(ThreadLocalRandom.current().nextInt(0,keys_num),ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
            }
        }
    }

    // removethread
    public static class removeThread extends Thread{
        int times;
        Map<Integer, Integer> hm;

        removeThread(int _times, Map<Integer, Integer> _hm){
            this.times = _times;
            this.hm = _hm;
        }

        public void run(){
            for(int i = 0; i < times; i++){
                // System.out.println("r " + hs.remove(i));
                hm.remove(ThreadLocalRandom.current().nextInt(0,keys_num));
            }
        }
    }

    //contiansthread
    public static class containsThread extends Thread{
        int times;
        Map<Integer, Integer> hm;

        containsThread(int _times, Map<Integer, Integer> _hm){
            this.times = _times;
            this.hm = _hm;
        }

        public void run(){
            for(int i = 0; i < times; i++){
                // System.out.println("c " + hs.contains(i));
                hm.containsKey(ThreadLocalRandom.current().nextInt(0, keys_num));
            }
        }
    }

    //getthread
    public static class getThread extends Thread{
        int times;
        Map<Integer, Integer> hm;

        getThread(int _times, Map<Integer, Integer> _hm){
            this.times = _times;
            this.hm = _hm;
        }

        public void run(){
            for(int i = 0; i < times; i++){
                // System.out.println("c " + hs.contains(i));
                hm.get(ThreadLocalRandom.current().nextInt(0, keys_num));
            }
        }
    }

    //main
    public static void main(String[] a) throws InterruptedException {
		/*
		 * This part used to table the testing parameter as you like
		 */
        int num_of_addthread = 9;
        int num_per_add = 1000;
        int num_of_removethread = 1;
        int num_per_remove = 1000;
        int num_of_containsKeythread = 90;
        int num_per_containsKey = 1000;
        int num_of_getthread = 90;
        int num_per_get = 1000;

        int size_of_hashtable = 1000;
        int max_num_buckets = 100000;

        int runs_per_algorithm = 5;

		/*
		 * int curHashTableIndex;
		 *
		 * Index for hashtable in hashtableList:
		 * 0. LockFreeHashTable              Lock-free, split ordered
		 * 1. CoarseHashTable                Lock-based, coarse grained
		 * 2. RefinableHashTable             Lock-based, fine grained, lock array resizable
		 */
        ArrayList<Map<Integer, Integer>> hashtableList = new ArrayList<Map<Integer, Integer>>();
        hashtableList.add(new LockFreeHashTable<>(max_num_buckets));
        hashtableList.add(new CoarseHashTable<>(size_of_hashtable));
        hashtableList.add(new RefinableHashTable<>(size_of_hashtable));


		/*
		 * threadpool for synchronization
		 */
        for (int curHashTableIndex = 0; curHashTableIndex < hashtableList.size(); curHashTableIndex += 1) {
            long totalTime = 0;
            for (int curRun = 0; curRun < runs_per_algorithm; curRun += 1) {
                ArrayList<Thread> threadPool = new ArrayList<Thread>();
                Thread curThread;

                long startTime = System.nanoTime();
                // put thread
                for(int i = 0; i < num_of_addthread; i++){
                    curThread = new putThread(num_per_add, hashtableList.get(curHashTableIndex));
                    threadPool.add(curThread);
                    curThread.start();
                }
                // get thread
                for(int i = 0; i < num_of_addthread; i++){
                    curThread = new putThread(num_per_get, hashtableList.get(curHashTableIndex));
                    threadPool.add(curThread);
                    curThread.start();
                }
                //containsKey thread
                for(int i = 0; i < num_of_containsKeythread; i++){
                    curThread = new containsThread(num_per_containsKey, hashtableList.get(curHashTableIndex));
                    threadPool.add(curThread);
                    curThread.start();
                }
                //remove thread
                for(int i = 0; i < num_of_removethread; i++){
                    curThread = new removeThread(num_per_remove, hashtableList.get(curHashTableIndex));
                    threadPool.add(curThread);
                    curThread.start();
                }
                // synchronization
                for(Thread t: threadPool){
                    t.join();
                }

                // time calculation
                long estimatedTime = System.nanoTime() - startTime;
                totalTime += estimatedTime;
            }

            // Display the current hashing algorithm
            switch (curHashTableIndex) {
                case 0: System.out.println("LockFreeHashTable -----------------------------"); break;
                case 1: System.out.println("CoarseHashTable -------------------------------"); break;
                case 2: System.out.println("RefinableHashTable ----------------------------"); break;
            }

            System.out.println("- Time Used: " + (double)totalTime/runs_per_algorithm/1000000 + " Milliseconds");
        }
    }
}
