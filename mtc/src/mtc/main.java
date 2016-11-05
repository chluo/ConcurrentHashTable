package mtc;

import java.util.ArrayList;

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
				System.out.println("a " + hs.add(i));
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
				System.out.println("r " + hs.remove(i));
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
				System.out.println("c " + hs.contains(i));
			}
		}
	}
	
	//main
	public static void main(String[] a) throws InterruptedException {
		/*
		 * This part used to set the testing parameter as you like
		 */
		int num_of_addthread = 1;
		int num_per_add = 100;
		int num_of_removethread = 1;
		int num_per_remove = 100;
		int num_of_containsthread = 1;
		int num_per_contains = 100;
		
		int size_of_hashset = 1000;
		
		int curHashSetIndex = 0;
		/*
		 * Index for hashset in hashsetList:
		 * 0. LockFreeHashSet
		 * 1. CoarseHashSet
		 * 2. RefinableHashSet
		 * 3. StripedHashSet
		 * 4. TCuckooHashSet
		 * 5. StripedCuckooHashSet
		 * 6. RefinableCuckooHashSet
		 * 7. CoarseCuckooHashSet
		 */
		ArrayList<Set<Integer>> hashsetList = new ArrayList<Set<Integer>>();
		hashsetList.add(new LockFreeHashSet<Integer>(size_of_hashset));
		hashsetList.add(new CoarseHashSet<Integer>(size_of_hashset));
		hashsetList.add(new RefinableHashSet<Integer>(size_of_hashset));
		hashsetList.add(new StripedHashSet<Integer>(size_of_hashset));
		hashsetList.add(new TCuckooHashSet<Integer>(size_of_hashset));
		hashsetList.add(new StripedCuckooHashSet<Integer>(size_of_hashset));
		hashsetList.add(new RefinableCuckooHashSet<Integer>(size_of_hashset));
		hashsetList.add(new CoarseCuckooHashSet<Integer>(size_of_hashset));
		
		/*
		 * threadpool for synchronization
		 */
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
		System.out.println("Time Used: " + estimatedTime/1000000 + " Milliseconds");

    }
}
