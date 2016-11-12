package HashTable;

import java.lang.ref.Reference;
import java.util.Iterator;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Liheng on 11/9/16.
 */
public class CoarseCuckooHashTable<K,V> implements Map<K,V> {
    protected Entry<K,V>[][] table;
    protected Lock lock;
    protected int size;
    protected static final int LIMIT = 32;
    // used for rehashing
    private Random random;

    public CoarseCuckooHashTable(int capacity) {
        lock = new ReentrantLock();
        table = (Entry<K, V>[][]) new Entry[2][capacity];
        size = capacity;
        random = new Random();
    }
    private final int hash0(K key) {
        return key.hashCode() % size;
    }
    private final int hash1(K key) {
        random.setSeed(key.hashCode());
        return random.nextInt(size);
    }
    public boolean containsKey(K key) {
        lock.lock();
        try {
            if (table[0][hash0(key)]!=null && key.equals(table[0][hash0(key)].key)) {
                return true;
            } else if (table[1][hash1(key)]!=null && key.equals(table[1][hash1(key)].key)) {
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    public V put(K key, V value) {
        lock.lock();
        try {
            if (containsKey(key)) {
                if (key.equals(table[0][hash0(key)].key)) {
                    V result = table[0][hash0(key)].value;
                    table[0][hash0(key)].value = value;
                    return result;
                } else if (key.equals(table[1][hash1(key)].key)) {
                    V result = table[1][hash1(key)].value;
                    table[1][hash1(key)].value = value;
                    return result;
                }
            }
            Entry<K,V> e = new Entry<K, V>(key.hashCode(),key,value);
            for (int i = 0; i < LIMIT; i++) {
                e = swap(0, hash0(key), e);
                if ( e == null) {
                    return null;
                }
                e = swap(1, hash1(key), e);
                if ( e == null) {
                    return null;
                }
            }
            System.out.println("uh-oh");
            throw new CoarseCuckooHashTable.CuckooException();
            //return null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public V get(K key) {
        lock.lock();
        V result = null;
        int i0 = hash0(key);
        int i1 = hash1(key);
        try {
            if (key.equals(table[0][i0])) {
                result = table[0][i0].value;
                return result;
            } else if (key.equals(table[1][i1])) {
                result = table[1][i1].value;
                return result;
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    public V remove(K key) {
        lock.lock();
        V result = null;
        int i0 = hash0(key);
        int i1 = hash1(key);
        try {
            if (key.equals(table[0][i0])) {
                result = table[0][i0].value;
                table[0][i0] = null;
                return result;
            } else if (key.equals(table[1][i1])) {
                result = table[1][i1].value;
                table[1][i1] = null;
                return result;
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    private Entry<K, V> swap(int i, int j, Entry<K,V> entry) {
        Entry<K,V> e = table[i][j];
        table[i][j] = entry;
        return e;
    }
    public static class CuckooException extends java.lang.RuntimeException {}
}
