package HashTable;

/**
 * Created by Liheng on 11/7/16.
 */
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

public class main {
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
                hm.put(ThreadLocalRandom.current().nextInt(0,keys_num), 1);
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
    public static void Lmain(String[] a) throws InterruptedException {
		/*
		 * This part used to table the testing parameter as you like
		 */
        int num_of_addthread = 6;
        int num_per_add = 10000;
        int num_of_removethread = 6;
        int num_per_remove = 10000;
        // int num_of_containsKeythread = 0;
        // int num_per_containsKey = 0;
        int num_of_getthread = 0;
        int num_per_get = 1000;

        int size_of_hashtable = 1000;
        int max_num_buckets = 100000;

        /** 
         * Number of tests for average 
         */
        int runs_per_algorithm = 10;

		/**
		 * int curHashTableIndex;
		 *
		 * Index for hashtable in hashtableList:
		 * 0. LockFreeHashTable              Lock-free, split ordered
		 * 1. CoarseHashTable                Lock-based, coarse grained
		 * 2. RefinableHashTable             Lock-based, fine grained, lock array resizable
		 * 3. RefinableDirCuckooHashTable
		 * 4. StripedCuckooHashTable
		 * 5. RefinableCuckooHashTable 
		 * 6. RefinableAssociativeCuckooHashTable
		 * 7. RefinableNRECuckooHashTable
		 * 8. RefinableLRECuckooHashTable
		 */
        ArrayList<Map<Integer, Integer>> hashtableList = new ArrayList<Map<Integer, Integer>>();
        /* 0 */ hashtableList.add(new LockFreeHashTable<Integer, Integer>(max_num_buckets));
        /* 1 */ hashtableList.add(new CoarseHashTable<Integer, Integer>(size_of_hashtable));
        /* 2 */ hashtableList.add(new RefinableHashTable<Integer, Integer>(size_of_hashtable));
        /* 3 */ hashtableList.add(new RefinableDirCuckooHashTable<Integer, Integer>(size_of_hashtable)); 
        /* 4 */ hashtableList.add(new StripedCuckooHashTable<Integer, Integer>(size_of_hashtable));
        /* 5 */ hashtableList.add(new RefinableCuckooHashTable<Integer, Integer>(size_of_hashtable));
        /* 6 */ hashtableList.add(new RefinableAssociativeCuckooHashTable<Integer, Integer>(size_of_hashtable)); 
        /* 7 */ hashtableList.add(new RefinableNRECuckooHashTable<Integer, Integer>(size_of_hashtable)); 
        /* 8 */ hashtableList.add(new RefinableLRECuckooHashTable<Integer, Integer>(size_of_hashtable)); 

		/*
		 * threadpool for synchronization
		 */
        int testSet[] = {1, 2, 3, 5, 6, 7, 8}; 
        for (int curHashTableIndex : testSet) {
            // long totalTime = 0;
            ArrayList<Long> resArray = new ArrayList<>(); 
            for (int curRun = 0; curRun < runs_per_algorithm; curRun += 1) {
                ArrayList<Thread> threadPPool = new ArrayList<Thread>();
                ArrayList<Thread> threadGPool = new ArrayList<Thread>();
                ArrayList<Thread> threadRPool = new ArrayList<Thread>();
                Thread curThread;

                
                // put thread
                for(int i = 0; i < num_of_addthread; i++){
                    curThread = new putThread(num_per_add, hashtableList.get(curHashTableIndex));
                    threadPPool.add(curThread);
                }
                // get thread
                for(int i = 0; i < num_of_addthread; i++){
                    curThread = new getThread(num_per_get, hashtableList.get(curHashTableIndex));
                    threadGPool.add(curThread);
                }
                //remove thread
                for(int i = 0; i < num_of_removethread; i++){
                    curThread = new removeThread(num_per_remove, hashtableList.get(curHashTableIndex));
                    threadRPool.add(curThread);
                }
                
                long startTime = System.nanoTime();
                // put thread
                for(Thread th : threadPPool){
                    th.start();
                }
                // get thread
                for(Thread th : threadGPool){
                    th.start();
                }
                //remove thread
                for(Thread th : threadRPool){
                    th.start();
                }
                
                // synchronization
                for(Thread th: threadPPool){
                    th.join();
                }    
                for(Thread th: threadGPool){
                    th.join();
                } 
                for(Thread th: threadRPool){
                    th.join();
                } 

                // time calculation
                long estimatedTime = System.nanoTime() - startTime;
                // totalTime += estimatedTime;
                resArray.add(estimatedTime);
            }

            // Display the current hashing algorithm
            System.out.println(); 
            switch (curHashTableIndex) {
            	case 0: System.out.println("LockFreeHashTable -----------------------------"); break;
            	case 1: System.out.println("CoarseHashTable -------------------------------"); break;
            	case 2: System.out.println("RefinableHashTable ----------------------------"); break;
            	case 3: System.out.println("RefinableDirCuckooHashTable -------------------"); break;
            	case 4: System.out.println("StripedCuckooHashTable ------------------------"); break;
            	case 5: System.out.println("RefinableCuckooHashTable ----------------------"); break;
            	case 6: System.out.println("RefinableAssociativeCuckooHashTable -----------"); break;
            	case 7: System.out.println("RefinableNRECuckooHashTable -------------------"); break;
            	case 8: System.out.println("RefinableLRECuckooHashTable -------------------"); break;
            }

            // remove the max and min sample 
            // resArray.remove(Collections.max(resArray)); 
            resArray.remove(Collections.max(resArray)); 
            // resArray.remove(Collections.min(resArray)); 
            resArray.remove(Collections.min(resArray)); 
            
            int num_op = num_of_addthread * num_per_add + 
            		num_of_removethread * num_per_remove + 
            		// num_of_containsKeythread * num_per_containsKey + 
            		num_of_getthread + num_per_get; 
            
            double avgTime = 0; 
            for (long res : resArray) {
            	avgTime += res; 
            }
            avgTime /= resArray.size(); 
            
            double avgTput = 0; 
            for (long res : resArray) {
            	avgTput += (double)num_op/res;  
            }
            avgTput /= resArray.size(); 

            // System.out.println("- Average Time Used: " + avgTime/1000000 + " ms");
            System.out.println("- Average Throughput: " + avgTput * 1000 + " ops/us");
            
//            System.out.println("- Throughput Samples: "); 
//            for (long res : resArray) {
//            	System.out.print((double)1000* num_thread* num_op_per_thread/res + " "); 
//            }
//            
            double TputRange = (double)1000* num_op/Collections.min(resArray) -
            		(double)1000* num_op/Collections.max(resArray); 
            System.out.println("- Throughput Range: " + TputRange); 
            // System.out.println("- Max Throughput: " + (double)1000* num_thread* num_op_per_thread/Collections.min(resArray)); 
            // System.out.println(); 
        }
    }
}