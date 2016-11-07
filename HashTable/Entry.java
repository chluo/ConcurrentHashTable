/**
 * Created by Liheng on 11/7/16.
 */
public class Entry<K,V> {
    int hash;
    K key;
    V value;

    protected  Entry(int hash, K key, V value) {
        this.hash = hash;
        this.key = key;
        this.value = value;
    }
}
