package mtc;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class main {
	
	// addthread
	public static class addThread extends Thread{
		int times;
		Set<Integer> hs;
		
		addThread(int _times, Set<Integer> _hs){
			this.times = _times;
			this.hs = _hs;
		}
		
		public void run(){
			for(int i = 0; i < times; i++){
				// System.out.println("a " + hs.add(i));
				hs.add(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE)); 
			}
		}
	}
	
	// removethread
	public static class removeThread extends Thread{
		int times;
		Set<Integer> hs;
		
		removeThread(int _times, Set<Integer> _hs){
			this.times = _times;
			this.hs = _hs;
		}
		
		public void run(){
			for(int i = 0; i < times; i++){
				// System.out.println("r " + hs.remove(i));
				hs.remove(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE)); 
			}
		}
	}
	
	//contiansthread
	public static class containsThread extends Thread{
		int times;
		Set<Integer> hs;
		
		containsThread(int _times, Set<Integer> _hs){
			this.times = _times;
			this.hs = _hs;
		}
		
		public void run(){
			for(int i = 0; i < times; i++){
				// System.out.println("c " + hs.contains(i));
				hs.contains(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE)); 
			}
		}
	}
	
	//main
	public static void main(String[] a) throws InterruptedException {
		/*
		 * This part used to set the testing parameter as you like
		 */
		int num_of_addthread = 9;
		int num_per_add = 1000;
		int num_of_removethread = 1;
		int num_per_remove = 1000;
		int num_of_containsthread = 90;
		int num_per_contains = 1000;
		
		int size_of_hashset = 1000;
		int max_num_buckets = 100000; 
		
		int runs_per_algorithm = 5;  
		
		/*
		 * int curHashSetIndex; 
		 * 
		 * Index for hashset in hashsetList:
		 * 0. LockFreeHashSet              Lock-free, split ordered 
		 * 1. CoarseHashSet                Lock-based, coarse grained 
		 * 2. StripedHashSet               Lock-based, fine grained, lock array unresizable
		 * 3. RWStripedHashSet             Lock-based, fine grained, lock array unresizable, using R/W lock for resizing
		 * 4. RefinableHashSet             Lock-based, fine grained, lock array resizable 
		 * 5. StripedCuckooHashSet         Lock-based, fine grained, lock array unresizable, Cuckoo hashing 
		 * 6. TCuckooHashSet               Lock-based, fine grained, lock array unresizable, Cuckoo hashing 
		 * 7. RefinableCuckooHashSet       Lock-based, fine grained, lock array resizable, Cuckoo hashing
		 * 8. ConcurrentHopscotchHashMap   (TODO) 
		 */
		ArrayList<Set<Integer>> hashsetList = new ArrayList<Set<Integer>>();
		hashsetList.add(new LockFreeHashSet<Integer>(max_num_buckets));
		hashsetList.add(new CoarseHashSet<Integer>(size_of_hashset));
		hashsetList.add(new StripedHashSet<Integer>(size_of_hashset));
		hashsetList.add(new RWStripedHashSet<Integer>(size_of_hashset));
		hashsetList.add(new RefinableHashSet<Integer>(size_of_hashset));
		hashsetList.add(new StripedCuckooHashSet<Integer>(size_of_hashset));
		hashsetList.add(new TCuckooHashSet<Integer>(size_of_hashset));
		hashsetList.add(new RefinableCuckooHashSet<Integer>(size_of_hashset));
		
		
		/*
		 * threadpool for synchronization
		 */
		for (int curHashSetIndex = 0; curHashSetIndex < hashsetList.size(); curHashSetIndex += 1) {
			long totalTime = 0; 
			for (int curRun = 0; curRun < runs_per_algorithm; curRun += 1) {
				ArrayList<Thread> threadPool = new ArrayList<Thread>();
				Thread curThread;

				long startTime = System.nanoTime(); 
				// add thread
				for(int i = 0; i < num_of_addthread; i++){
					curThread = new addThread(num_per_add, hashsetList.get(curHashSetIndex));
					threadPool.add(curThread);
					curThread.start();
				}
				//remove thread
				for(int i = 0; i < num_of_removethread; i++){
					curThread = new removeThread(num_per_remove, hashsetList.get(curHashSetIndex));
					threadPool.add(curThread);
					curThread.start();
				}
				//contains thread
				for(int i = 0; i < num_of_containsthread; i++){
					curThread = new containsThread(num_per_contains, hashsetList.get(curHashSetIndex));
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
			switch (curHashSetIndex) {
			case 0: System.out.println("LockFreeHashSet -----------------------------"); break; 
			case 1: System.out.println("CoarseHashSet -------------------------------"); break; 
			case 2: System.out.println("StripedHashSet ------------------------------"); break; 
			case 3: System.out.println("RWStripedHashSet ----------------------------"); break; 
			case 4: System.out.println("RefinableHashSet ----------------------------"); break; 
			case 5: System.out.println("StripedCuckooHashSet ------------------------"); break; 
			case 6: System.out.println("TCuckooHashSet ------------------------------"); break; 
			case 7: System.out.println("RefinableCuckooHashSet ----------------------"); break; 
			}
			
			System.out.println("- Time Used: " + (double)totalTime/runs_per_algorithm/1000000 + " Milliseconds");
		}
    }
}
