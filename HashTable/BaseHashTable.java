package HashTable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Liheng on 11/6/16.
 */

public abstract class BaseHashTable<K,V> implements Map<K,V>{
    protected List<Entry<K,V>>[] table;
    protected int size;

    public BaseHashTable(int capacity) {
        size = 0;
        table = (List<Entry<K,V>>[]) new List[capacity];
        for(int i = 0; i < capacity; i++) {
            table[i] = new ArrayList<Entry<K,V>>();
        }
    }

    @Override
    public V put(K key, V value) {
        acquire(key);
        try {
            if(value==null) {
                throw new NullPointerException();
            }
            int hash = key.hashCode();
            int index = (hash & 0x7FFFFFFF) % table.length;
            for(Entry<K,V> e: table[index]) {
                if(e.hash==hash && e.key.equals(key)) {
                    V oldValue = e.value;
                    e.value = value;
                    return oldValue;
                }
            }
            table[index].add(new Entry<K, V>(hash, key, value));
            size++;
        }finally {
          release(key);
        }
        if (policy())
            resize();
        return null;
    }

    @Override
    public V get(K key) {
        acquire(key);
        try {
            int hash = key.hashCode();
            int index = (hash & 0x7FFFFFFF) % table.length;
            for (Entry<K, V> e : table[index]) {
                if (e.hash == hash && e.key.equals(key)) {
                    return e.value;
                }
            }
        }finally {
            release(key);
        }
        return null;
    }

    @Override
    public V remove(K key) {
        acquire(key);
        try {
            int hash = key.hashCode();
            int index = (hash & 0x7FFFFFFF) % table.length;
            for(Entry<K,V> e: table[index]) {
                if (e.hash == hash && e.key.equals(key)) {
                    V result = e.value;
                    table[index].remove(e);
                    size--;
                    return result;
                }
            }
        }finally {
            release(key);
        }
        return null;
    }

    @Override
    public boolean containsKey(K key) {
        acquire(key);
        try {
            int hash = key.hashCode();
            int index = (hash & 0x7FFFFFFF) % table.length;
            for(Entry<K,V> e: table[index]) {
                if (e.hash == hash && e.key.equals(key)) {
                    return true;
                }
            }
        }finally {
            release(key);
        }
        return false;
    }

    public abstract void acquire(K key);

    public abstract void release(K key);

    public abstract void resize();

    public abstract boolean policy();
}
