package HashTable;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Liheng on 11/6/16.
 */
public class RefinableHashTable<K,V> extends BaseHashTable<K,V> {
    AtomicMarkableReference<Thread> owner;
    volatile ReentrantLock[] locks;

    public RefinableHashTable (int capacity) {
        super(capacity);
        locks = new ReentrantLock[capacity];
        for (int j = 0; j < capacity; j++) {
            locks[j] = new ReentrantLock();
        }
        owner = new AtomicMarkableReference<Thread>(null, false);
    }

    public void acquire(K key) {
        boolean[] mark = {true};
        Thread me = Thread.currentThread();
        Thread who;
        while (true) {
            do {
                who = owner.get(mark);
            } while (mark[0] && who != me);
            ReentrantLock[] oldLocks = this.locks;
            int index = (key.hashCode() & 0x7FFFFFFF) % table.length;
            ReentrantLock oldLock = oldLocks[index];
            oldLock.lock();
            who = owner.get(mark);
            if ((!mark[0] || who == me) && this.locks == oldLocks) { // recheck
                return;
            } else {  //  unlock & try again
                oldLock.unlock();
            }
        }
    }

    public void release(K key) {
        int index = (key.hashCode() & 0x7FFFFFFF) % table.length;
        locks[index].unlock();
    }

    /**
     * Ensure that no thread is currently locking the table.
     */
    protected void quiesce() {
        for (ReentrantLock lock : locks) {
            while (lock.isLocked()) {}  // spin
        }
    }

    public void resize() {
        int oldCapacity = table.length;
        int newCapacity = 2 * oldCapacity;
        Thread me = Thread.currentThread();
        if (owner.compareAndSet(null, me, false, true)) {
            try {
                if (table.length != oldCapacity) {  // someone else resized first
                    return;
                }
                quiesce();
                List<Entry<K,V>>[] oldTable = table;
                table = (List<Entry<K,V>>[]) new List[newCapacity];
                for (int i = 0; i < newCapacity; i++)
                    table[i] = new ArrayList<Entry<K,V>>();
                locks = new ReentrantLock[newCapacity];
                for (int j = 0; j < locks.length; j++) {
                    locks[j] = new ReentrantLock();
                }
                initializeFrom(oldTable);
            } finally {
                owner.set(null, false);       // restore prior state
            }
        }
    }

    public boolean policy() {
        return size / table.length > 4;
    }

    private void initializeFrom(List<Entry<K,V>>[] oldTable) {
        for (List<Entry<K,V>> bucket : oldTable) {
            for (Entry<K,V> e : bucket) {
                int index = (e.key.hashCode() & 0x7FFFFFFF) % table.length;
                table[index].add(e);
            }
        }
    }
}
