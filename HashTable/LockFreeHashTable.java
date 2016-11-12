package HashTable;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Liheng on 11/7/16.
 */
public class LockFreeHashTable<K,V> implements Map<K,V>{
    protected SplitOrderedList<K,V>[] bucket;
    protected AtomicInteger bucketSize;
    protected AtomicInteger setSize;
    private static final double THRESHOLD = 4.0;
    /**
     * Constructor
     * @param capacity max number of bucket
     */
    public LockFreeHashTable(int capacity) {
        bucket = (SplitOrderedList<K,V>[]) new SplitOrderedList[capacity];
        bucket[0] = new SplitOrderedList<K,V>();
        bucketSize = new AtomicInteger(2);
        setSize = new AtomicInteger(0);
    }

    @Override
    public V put(K key, V value) {
        int myBucket = Math.abs(SplitOrderedList.hashCode(key) % bucketSize.get());
        SplitOrderedList<K,V> b = getSplitOrderedList(myBucket);
        V result = b.put(key, value);
        if (result==null)
            return null;
        int setSizeNow = setSize.getAndIncrement();
        int bucketSizeNow = bucketSize.get();
        if (setSizeNow / (double)bucketSizeNow > THRESHOLD)
            bucketSize.compareAndSet(bucketSizeNow, 2 * bucketSizeNow);
        return result;
    }

    @Override
    public V get(K key) {
        int myBucket = Math.abs(SplitOrderedList.hashCode(key) % bucketSize.get());
        SplitOrderedList<K,V> b = getSplitOrderedList(myBucket);
        V result = b.get(key);
        if (result==null)
            return null;
        return result;
    }

    @Override
    public V remove(K key) {
        int myBucket = Math.abs(BucketList.hashCode(key) % bucketSize.get());
        SplitOrderedList<K,V> b = getSplitOrderedList(myBucket);
        V result = b.remove(key);
        if (result==null) {
            return null;		// she's not there
        }
        return result;
    }
    @Override
    public boolean containsKey(K key) {
        int myBucket = Math.abs(BucketList.hashCode(key) % bucketSize.get());
        SplitOrderedList<K,V> b = getSplitOrderedList(myBucket);
        return b.containsKey(key);
    }
    private SplitOrderedList<K,V> getSplitOrderedList(int myBucket) {
        if (bucket[myBucket] == null)
            initializeBucket(myBucket);
        return bucket[myBucket];
    }
    private void initializeBucket(int myBucket) {
        int parent = getParent(myBucket);
        if (bucket[parent] == null)
            initializeBucket(parent);
        SplitOrderedList<K,V> b = bucket[parent].getSentinel(myBucket);
        if (b != null)
            bucket[myBucket] = b;
    }
    private int getParent(int myBucket){
        int parent = bucketSize.get();
        do {
            parent = parent >> 1;
        } while (parent > myBucket);
        parent = myBucket - parent;
        return parent;
    }
}
