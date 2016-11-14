package HashTable;

/**
 * Created by Liheng on 11/7/16.
 * Updated by Chunheng on 11/11/16. 
 */
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class newMain {
    // randThread 
    public static class randThread implements Runnable {
    	int num_op; 
    	int key_range; 
    	int get_perc; 
    	int put_perc; 
    	int remove_perc; 
    	Map<Integer, Integer> hm; 
    	ArrayList<Integer> randArray; 
    	
    	randThread(int _num_op, int _key_range, int _get_p, int _put_p, int _remove_p, ArrayList<Integer> _randArray, Map<Integer, Integer> _hm) {
    		this.num_op = _num_op; 
    		this.key_range = _key_range; 
    		this.get_perc = _get_p; 
    		this.put_perc = _put_p; 
    		this.remove_perc = _remove_p; 
    		this.hm = _hm; 
    		this.randArray = _randArray;  
    	}
    	
    	public void run() {
    		for(int i = 0; i < num_op; i++) {
    			int randInt = ThreadLocalRandom.current().nextInt(0, 100); 
    			if (randInt < get_perc) {
    				// System.out.println("get"); 
    				hm.get(randArray.get(i)); 
    			}
    			else if (randInt < get_perc + put_perc) {
    				// System.out.println("put"); 
    				hm.put(randArray.get(i), 1);
    			}
    			else {
    				// System.out.println("remove");
    				hm.remove(randArray.get(i));				
    			}
    			// System.out.print(100*(double)i/(num_op - 1) + " ");
    		}
    	}
    }
    
    // main
    public static void main(String[] a) throws InterruptedException {
		/**
		 * This part used to table the testing parameter as you like
		 */
        int num_thread = 32; 
        int num_op_per_thread = 1_000_000; 
        int key_range = (int) Math.pow(2, 11); 
        int get_perc = 0; 
        int put_perc = 50; 
        int remove_perc = 50;

        /** 
         * Hash table configurations 
         */
        int size_of_hashtable = 8;
        int max_num_buckets = 1_000_000;
        
        /** 
         * Number of tests for average 
         */
        int runs_per_algorithm = 5;

		/**
		 * int curHashTableIndex;
		 *
		 * Index for hashtable in hashtableList ... 
		 * 0. LockFreeHashTable              Lock-free, split ordered
		 * 1. CoarseHashTable                Lock-based, coarse grained
		 * 2. RefinableHashTable             Lock-based, fine grained, lock array resizable
		 * 3. RefinableDirCuckooHashTable
		 * 4. StripedCuckooHashTable
		 * 5. RefinableCuckooHashTable 
		 * 6. RefinableAssociativeCuckooHashTable
		 * 7. RefinableNRECuckooHashTable
		 * 8. RefinableLRECuckooHashTable
		 * 9. ThreadSafeCuckooHashMap        Lock-free
		 *10. ConcurrentHopscotchHashMap
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
        /* 9 */ hashtableList.add(new ThreadSafeCuckooHashMap<Integer, Integer>(8*max_num_buckets));
        /*10 */ hashtableList.add(new ConcurrentHopscotchHashMap<Integer, Integer>(max_num_buckets, num_thread));

		/**
		 * threadpool for synchronization
		 */
        int testSet[] = {1, 2, 3, 5, 6, 7, 8}; 
        // int testSet[] = {/*0,*/ 10}; 
        // int testSet[] = {9};
        
        // Random numbers to be applied to the hash tables under test
        ArrayList<ArrayList<Integer>> randArray = new ArrayList<>(); 
        for (int j = 0; j < num_thread; j++) {
        	randArray.add(new ArrayList<Integer>()); 
        	for (int i = 0; i < num_op_per_thread; i++) {
        		randArray.get(j).add(ThreadLocalRandom.current().nextInt(0, key_range)); 
        	}
        }
		
        for (int curHashTableIndex : testSet) {
            ArrayList<Long> resArray = new ArrayList<>(); 
            for (int curRun = 0; curRun < runs_per_algorithm; curRun += 1) {
                ArrayList<Thread> threadPool = new ArrayList<Thread>();
                Thread curThread;

                
                
                // randThread
                for(int i = 0; i < num_thread; i++){
                    curThread = new Thread(
                    		new randThread(
                    				num_op_per_thread, 
                    				key_range, 
                    				get_perc, put_perc, 
                    				remove_perc, 
                    				randArray.get(i), 
                    				hashtableList.get(curHashTableIndex)
                    				)
                    		);
                    threadPool.add(curThread);
                    // curThread.start();
                }
                
                long startTime = System.nanoTime();
                for (Thread th : threadPool) {
                	th.start(); 
                }
                // synchronization
                for(Thread t: threadPool){
                    t.join();
                }

                // time calculation
                long estimatedTime = System.nanoTime() - startTime;
                resArray.add(estimatedTime); 
                
                // have a rest
                TimeUnit.SECONDS.sleep(1);
            }

            // Display the current hashing algorithm
            System.out.println(); 
            switch (curHashTableIndex) {
                case 0 : System.out.println("LockFreeHashTable -----------------------------"); break;
                case 1 : System.out.println("CoarseHashTable -------------------------------"); break;
                case 2 : System.out.println("RefinableHashTable ----------------------------"); break;
                case 3 : System.out.println("RefinableDirCuckooHashTable -------------------"); break;
                case 4 : System.out.println("StripedCuckooHashTable ------------------------"); break;
                case 5 : System.out.println("RefinableCuckooHashTable ----------------------"); break;
                case 6 : System.out.println("RefinableAssociativeCuckooHashTable -----------"); break;
                case 7 : System.out.println("RefinableNRECuckooHashTable -------------------"); break;
                case 8 : System.out.println("RefinableLRECuckooHashTable -------------------"); break;
                case 9 : System.out.println("ThreadSafeCuckooHashMap -----------------------"); break;
                case 10: System.out.println("ConcurrentHopscotchHashMap --------------------"); break;
            }
            
            // remove the max and min sample 
            resArray.remove(Collections.max(resArray)); 
            resArray.remove(Collections.min(resArray)); 
            
            double avgTime = 0; 
            for (long res : resArray) {
            	avgTime += res; 
            }
            avgTime /= resArray.size(); 
            
            double avgTput = 0; 
            for (long res : resArray) {
            	avgTput += (double)num_thread* num_op_per_thread/res;  
            }
            avgTput /= resArray.size(); 

            // System.out.println("- Average Time Used: " + avgTime/1000000 + " ms");
            System.out.println("- Average Throughput: " + avgTput * 1000 + " ops/us");
            
//            System.out.println("- Throughput Samples: "); 
//            for (long res : resArray) {
//            	System.out.print((double)1000* num_thread* num_op_per_thread/res + " "); 
//            }
//            
//            double TputRange = (double)1000* num_thread* num_op_per_thread/Collections.min(resArray) -
//            		(double)1000* num_thread* num_op_per_thread/Collections.max(resArray); 
//            System.out.println("- Throughput Range: " + TputRange); 
            // System.out.println("- Max Throughput: " + (double)1000* num_thread* num_op_per_thread/Collections.min(resArray)); 
            // System.out.println(); 
            double stdev = 0; 
            double stsum = 0; 
            for (long res : resArray) {
            	double tput = (double)num_thread* num_op_per_thread/res; 
            	stsum += Math.pow((tput - avgTput), 2); 
            }
            stdev = Math.sqrt(stsum)/resArray.size(); 
            System.out.println("- Standard Deviation: " + stdev * 1000 + " ops/us");

        }
    }
}
