/*
 * StripedHashSet.java
 *
 * Created on November 15, 2006, 3:35 PM
 *
 * From "The Art of Multiprocessor Programming",
 * by Maurice Herlihy and Nir Shavit.
 *
 * This work is licensed under a Creative Commons Attribution-Share Alike 3.0 United States License.
 * http://i.creativecommons.org/l/by-sa/3.0/us/88x31.png
 */
//package xbird.util.concurrent.set;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Concurrent Cuckoo hashing using lock striping.
 * @param <T> type
 * @author Maurice Herlihy
 */
public class StripedHashSet<T> extends BaseHashSet<T> {
    final Lock[] locks;

    public StripedHashSet(int capacity) {
        super(capacity);
        locks = new Lock[capacity];
        for(int j = 0; j < locks.length; j++) {
            locks[j] = new ReentrantLock();
        }
    }

    /**
     * double the set size
     */
    @Override
    public void resize() {
        int oldCapacity = table.length;
        for(Lock lock : locks) {
            lock.lock();
        }
        try {
            if(oldCapacity != table.length) {
                return; // someone beat us to it
            }
            int newCapacity = 2 * oldCapacity;
            List<T>[] oldTable = table;
            table = (List<T>[]) new List[newCapacity];
            for(int i = 0; i < newCapacity; i++)
                table[i] = new ArrayList<T>();
            initializeFrom(oldTable);
        } finally {
            for(Lock lock : locks) {
                lock.unlock();
            }
        }
    }

    private void initializeFrom(List<T>[] oldTable) {
        for(List<T> bucket : oldTable) {
            for(T x : bucket) {
                int myBucket = Math.abs(x.hashCode() % table.length);
                table[myBucket].add(x);
            }
        }
    }

    /**
     * Synchronize before adding, removing, or testing for item
     * @param x item involved
     */
    @Override
    public final void acquire(T x) {
        int myBucket = Math.abs(x.hashCode() % locks.length);
        locks[myBucket].lock();
    }

    /**
     * synchronize after adding, removing, or testing for item
     * @param x item involved
     */
    @Override
    public void release(T x) {
        int myBucket = Math.abs(x.hashCode() % locks.length);
        locks[myBucket].unlock();
    }

    @Override
    public boolean policy() {
        return size / table.length > 4;
    }
}


/**
 * Simple fine-grained hash map.
 * @param <T> type
 * @author Maurice Herlihy
 */
 abstract class BaseHashSet<T> {
    protected List<T>[] table;
    protected int size;

    public BaseHashSet(int capacity) {
        size = 0;
        table = (List<T>[]) new List[capacity];
        for(int i = 0; i < capacity; i++) {
            table[i] = new ArrayList<T>();
        }
    }

    /**
     * Is item in set?
     * @param x item to test
     * @return <code>true</code> iff item present
     */
    public boolean contains(T x) {
        acquire(x);
        try {
            int myBucket = Math.abs(x.hashCode() % table.length);
            return table[myBucket].contains(x);
        } finally {
            release(x);
        }
    }

    /**
     * Add item to set
     * @param x item to add
     * @return <code>true</code> iff set changed
     */
    public boolean add(T x) {
        boolean result = false;
        acquire(x);
        try {
            int myBucket = Math.abs(x.hashCode() % table.length);
            result = table[myBucket].add(x);
            size = result ? size + 1 : size;
        } finally {
            release(x); // always unlock
        }
        if(policy())
            resize();
        return result;
    }

    /**
     * Remove item from set
     * @param x item to remove
     * @return <code>true</code> iff set changed
     */
    public boolean remove(T x) {
        acquire(x);
        try {
            int myBucket = Math.abs(x.hashCode() % table.length);
            boolean result = table[myBucket].remove(x);
            size = result ? size - 1 : size;
            return result;
        } finally {
            release(x); // always unlock
        }
    }

    /**
     * Synchronize before adding, removing, or testing for item
     * @param x item involved
     */
    public abstract void acquire(T x);

    /**
     * synchronize after adding, removing, or testing for item
     * @param x item involved
     */
    public abstract void release(T x);

    /**
     * double the set size
     */
    public abstract void resize();

    /**
     * decide whether to resize
     * @return whether to resize
     */
    public abstract boolean policy();

}