package HashTable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Chunheng on 11/11, 2016
 */
public class RefinableLRECuckooHashTable<K,V> extends LRECuckooHashTable<K,V> {
    AtomicMarkableReference<Thread> owner;
    volatile ReentrantLock[][] locks;

    public RefinableLRECuckooHashTable(int capacity) {
        super(capacity);
        locks = new ReentrantLock[2][capacity];
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < capacity; j++) {
                locks[i][j] = new ReentrantLock();
            }
        }
        owner = new AtomicMarkableReference<Thread>(null, false);
    }

    public void acquire(K key) {
        boolean[] mark = {true};
        Thread me = Thread.currentThread();
        Thread who;
        while (true) {
            do { // wait until not resizing
                who = owner.get(mark);
            } while (mark[0] && who != me);
            ReentrantLock[][] oldLocks = this.locks;
            ReentrantLock oldLock0 = oldLocks[0][hash0(key) % oldLocks[0].length];
            ReentrantLock oldLock1 = oldLocks[1][hash1(key) % oldLocks[1].length];
            oldLock0.lock();  // acquire locks
            oldLock1.lock();
            who = owner.get(mark);
            if ((!mark[0] || who == me) && this.locks == oldLocks) { // recheck
                return;
            } else {  //  unlock & try again
                oldLock0.unlock();
                oldLock1.unlock();
            }
        }
    }

    public final void release(K key) {
        Lock lock0 = locks[0][hash0(key) % locks[0].length];
        Lock lock1 = locks[1][hash1(key) % locks[1].length];
        lock0.unlock();
        lock1.unlock();
    }

    protected void quiesce() {
        for (ReentrantLock lock : locks[0]) {
            while (lock.isLocked()) {}  // spin
        }
    }
    /**
     * double the set size
     */
    public void resize() {
    	// System.out.println("Resize called"); 
        int oldCapacity = capacity;
        Thread me = Thread.currentThread();
        if (owner.compareAndSet(null, me, false, true)) {
            try {
                if (capacity != oldCapacity) {  // someone else resized first
                    return;
                }
                quiesce();
                capacity = 2 * capacity;
                ArrayList<CountedEntry<K,V>>[][] oldTable = table;
                table = (ArrayList<CountedEntry<K,V>>[][]) new ArrayList[2][capacity];
                locks = new ReentrantLock[2][capacity];
                for (int i = 0; i < 2; i++) {
                    for (int j = 0; j < capacity; j++) {
                        locks[i][j] = new ReentrantLock();
                    }
                }
                for (ArrayList<CountedEntry<K,V>>[] row : table) {
                    for (int i = 0; i < row.length; i++) {
                        row[i]  = new ArrayList<CountedEntry<K,V>>(PROBE_SIZE);
                    }
                }
                for (ArrayList<CountedEntry<K,V>>[] row : oldTable) {
                    for (ArrayList<CountedEntry<K,V>> set : row) {
                        for (CountedEntry<K,V> e : set) {
                            put(e.key, e.value);
                        }
                    }
                }
            } finally {
                owner.set(null, false);       // restore prior state
            }
        }
    }
}
