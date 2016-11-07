/**
 * Created by Liheng on 11/6/16.
 */

public interface Map<K,V> {
    /**
     * put key-value pair into map
     * @param key - key with which the specified value is to be associated
     * @param value - value to be associated with the specified key
     * @return the previous value associated with key, or null if there was no mapping for key
     */
    V put(K key, V value);

    /**
     * get key-value pair
     * @param key - the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or null if this map contains no mapping for the key
     */
    V get(K key);

    /**
     * remove key-value pair from map
     * @param key -  key whose mapping is to be removed from the map
     * @return the previous value associated with key, or null if there was no mapping for key
     */
    V remove(K key);

    /**
     * is key in map?
     * @param key - key whose presence in this map is to be tested
     * @return true if this map contains a mapping for the specified key
     */
    public boolean containsKey(K key);

    /**
     * is value in map?
     * @param value - value whose presence in this map is to be tested
     * @return true if this map maps one or more keys to the specified value
     */
//    public boolean containsValue(V value);
}
