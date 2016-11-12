package HashTable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Liheng on 11/9/16.
 */
public class StripedCuckooHashTable<K,V> extends PhasedCuckooHashTable<K,V> {
    final ReentrantLock[][] lock;

    public StripedCuckooHashTable(int capacity) {
        super(capacity);
        lock  = new ReentrantLock[2][capacity];
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < capacity; j++) {
                lock[i][j] = new ReentrantLock();
            }
        }
    }

    /**
     * double the set size
     */
    public void resize() {
        int oldCapacity = capacity;
        for (Lock _lock : lock[0]) {
            _lock.lock();
        }
        try {
            if (capacity != oldCapacity) {  // someone else resized first
                return;
            }
            List<Entry<K,V>>[][] oldTable = table;
            capacity = 2 * capacity;
            table = (List<Entry<K,V>>[][]) new List[2][capacity];
            for (List<Entry<K,V>>[] row : table) {
                for (int i = 0; i < row.length; i++) {
                    row[i] = new ArrayList<Entry<K,V>>(PROBE_SIZE);
                }
            }
            for (List<Entry<K,V>>[] row : oldTable) {
                for (List<Entry<K,V>> set : row) {
                    for (Entry<K,V> e : set) {
                        put(e.key, e.value);
                    }
                }
            }
        } finally {
            for (Lock _lock : lock[0]) {
                _lock.unlock();
            }
        }
    }

    public final void acquire(K key) {
        Lock lock0 = lock[0][hash0(key) % lock[0].length];
        Lock lock1 = lock[1][hash1(key) % lock[1].length];
        lock0.lock();
        lock1.lock();
    }

    public final void release(K key) {
        Lock lock0 = lock[0][hash0(key) % lock[0].length];
        Lock lock1 = lock[1][hash1(key) % lock[1].length];
        lock0.unlock();
        lock1.unlock();
    }
}
