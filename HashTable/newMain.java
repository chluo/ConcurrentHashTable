package HashTable;

/**
 * Created by Liheng on 11/7/16.
 * Updated by Chunheng on 11/11/16. 
 */
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class newMain {
    // final static int keys_num = 1000000;

    // randThread 
    public static class randThread extends Thread {
    	int num_op; 
    	int key_range; 
    	int get_perc; 
    	int put_perc; 
    	int remove_perc; 
    	Map<Integer, Integer> hm; 
    	
    	randThread(int _num_op, int _key_range, int _get_p, int _put_p, int _remove_p, Map<Integer, Integer> _hm) {
    		this.num_op = _num_op; 
    		this.key_range = _key_range; 
    		this.get_perc = _get_p; 
    		this.put_perc = _put_p; 
    		this.remove_perc = _remove_p; 
    		this.hm = _hm; 
    	}
    	
    	public void run() {
    		for(int i = 0; i < num_op; i++) {
    			int randInt = ThreadLocalRandom.current().nextInt(0, 100); 
    			if (randInt < get_perc)
    				hm.get(ThreadLocalRandom.current().nextInt(0, key_range)); 
    			else if (randInt < get_perc + put_perc) 
    				hm.put(ThreadLocalRandom.current().nextInt(0, key_range), 1);
    			else 
    				hm.remove(ThreadLocalRandom.current().nextInt(0, key_range));
    		}
    	}
    }
    
	/* 
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
                // hm.put(ThreadLocalRandom.current().nextInt(0,keys_num),ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
            	hm.put(i, i); 
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
                // hm.remove(ThreadLocalRandom.current().nextInt(0,keys_num));
            	hm.remove(i); 
            }
        }
    }

    // contiansthread
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
                // hm.containsKey(ThreadLocalRandom.current().nextInt(0, keys_num));
            	hm.containsKey(i); 
            }
        }
    }

    // getthread
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
                // hm.get(ThreadLocalRandom.current().nextInt(0, keys_num));
            	hm.get(i); 
            }
        }
    }
    */ 

    // main
    public static void main(String[] a) throws InterruptedException {
		/**
		 * This part used to table the testing parameter as you like
		 */
        int num_thread = 16; 
        int num_op_per_thread = 1_000_000; 
        int key_range = (int) Math.pow(2, 11); 
        int get_perc = 90; 
        int put_perc = 9; 
        int remove_perc = 1;
        
    	/*
        int num_of_addthread = 5;
        int num_per_add = 1000;
        int num_of_removethread = 5;
        int num_per_remove = 10000;
        int num_of_containsKeythread = 50;
        int num_per_containsKey = 1000;
        int num_of_getthread = 40;
        int num_per_get = 1000;   
        */

        /** 
         * Hash table configurations 
         */
        int size_of_hashtable = 1000;
        int max_num_buckets = 100000;
        
        /** 
         * Number of tests for average 
         */
        int runs_per_algorithm = 5;

		/**
		 * int curHashTableIndex;
		 *
		 * Index for hashtable in hashtableList:
		 * 0. LockFreeHashTable              Lock-free, split ordered
		 * 1. CoarseHashTable                Lock-based, coarse grained
		 * 2. RefinableHashTable             Lock-based, fine grained, lock array resizable
		 * 3. StripedCuckooHashTable
		 * 4. RefinableCuckooHashTable 
		 * 5. RefinableAssociativeCuckooHashTable
		 */
        ArrayList<Map<Integer, Integer>> hashtableList = new ArrayList<Map<Integer, Integer>>();
        hashtableList.add(new LockFreeHashTable<Integer, Integer>(max_num_buckets));
        hashtableList.add(new CoarseHashTable<Integer, Integer>(size_of_hashtable));
        hashtableList.add(new RefinableHashTable<Integer, Integer>(size_of_hashtable));
        hashtableList.add(new StripedCuckooHashTable<Integer, Integer>(size_of_hashtable));
        hashtableList.add(new RefinableCuckooHashTable<Integer, Integer>(size_of_hashtable));
        //hashtableList.add(new CoarseCuckooHashTable<Integer, Integer>(size_of_hashtable));
        hashtableList.add(new RefinableAssociativeCuckooHashTable<Integer, Integer>(size_of_hashtable)); 

		/**
		 * threadpool for synchronization
		 */
        for (int curHashTableIndex = 0; curHashTableIndex < hashtableList.size(); curHashTableIndex += 1) {
            long totalTime = 0;
            for (int curRun = 0; curRun < runs_per_algorithm; curRun += 1) {
                ArrayList<Thread> threadPool = new ArrayList<Thread>();
                Thread curThread;

                long startTime = System.nanoTime();
                
                /* 
                // put thread
                for(int i = 0; i < num_of_addthread; i++){
                    curThread = new putThread(num_per_add, hashtableList.get(curHashTableIndex));
                    threadPool.add(curThread);
                    curThread.start();
                }
                // get thread
                for(int i = 0; i < num_of_getthread; i++){
                    curThread = new getThread(num_per_get, hashtableList.get(curHashTableIndex));
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
                */ 
                // randThread
                for(int i = 0; i < num_thread; i++){
                    curThread = new randThread(num_op_per_thread, key_range, get_perc, put_perc, remove_perc, hashtableList.get(curHashTableIndex));
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
                case 3: System.out.println("StripedCuckooHashTable ------------------------"); break;
                case 4: System.out.println("RefinableCuckooHashTable ----------------------"); break;
                case 5: System.out.println("RefinableAssociativeCuckooHashTable -----------"); break;
                //case 4: System.out.println("CoarseCuckooHashTable ----------------------------"); break;
            }

            System.out.println("- Time Used: " + (double)totalTime/runs_per_algorithm/1000000 + " Milliseconds");
        }
    }
}
