import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Liheng on 11/7/16.
 */
public class CoarseHashTable<K,V> extends BaseHashTable<K,V> {
    final Lock lock;
    CoarseHashTable(int capacity) {
        super(capacity);
        lock = new ReentrantLock();
    }

    public void resize() {
        int oldCapacity = table.length;
        lock.lock();
        try {
            if (oldCapacity != table.length) {
                return;
            }
            int newCapacity = 2 * oldCapacity;
            List<Entry<K, V>>[] oldTable = table;
            table = (List<Entry<K,V>>[]) new List[newCapacity];
            for (int i = 0; i < newCapacity; i++) {
                table[i] = new ArrayList<Entry<K,V>>();
            }
            for (List<Entry<K,V>> bucket: oldTable) {
                for (Entry<K,V> e: bucket) {
                    int hash = e.key.hashCode();
                    int index = (hash & 0x7FFFFFFF) % table.length;
                    table[index].add(e);
                }
            }
        }finally {
            lock.unlock();
        }
    }

    public final void acquire(K key) {
        lock.lock();
    }

    public void release(K key) {
        lock.unlock();
    }

    public boolean policy() {
        return size / table.length>4;
    }
}

