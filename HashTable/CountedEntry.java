package HashTable;

/**
 * Created by Chunheng on 11/12/16.
 * Used for Least Relocated Entry (NRE) replacement policy 
 */
public class CountedEntry<K,V> {
    int hash;
    K key;
    V value;
    int cnt; 

    protected  CountedEntry(int hash, K key, V value) {
        this.hash = hash;
        this.key = key;
        this.value = value;
        this.cnt = 0; 
    }
}
