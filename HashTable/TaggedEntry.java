package HashTable;

/**
 * Created by Chunheng on 11/12/16.
 * Used for Not Relocated Entry (NRE) replacement policy 
 */
public class TaggedEntry<K,V> {
    int hash;
    K key;
    V value;
    boolean relocated; 

    protected  TaggedEntry(int hash, K key, V value) {
        this.hash = hash;
        this.key = key;
        this.value = value;
        this.relocated = false; 
    }
}
