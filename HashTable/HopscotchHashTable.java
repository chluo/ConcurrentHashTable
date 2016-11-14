//
//////////////////////////////////////////////////////////////////////////////////
//// Concurrent Hopscotch Hash Map
////
//////////////////////////////////////////////////////////////////////////////////
////TERMS OF USAGE
////----------------------------------------------------------------------
////
////  Permission to use, copy, modify and distribute this software and
////  its documentation for any purpose is hereby granted without fee,
////  provided that due acknowledgements to the authors are provided and
////  this permission notice appears in all copies of the software.
////  The software is provided "as is". There is no warranty of any kind.
////
////Programmers:
////  Hila Goel
////  Tel-Aviv University
////  and
////  Maya Gershovitz
////  Tel-Aviv University
////
////
////  Date: January, 2015.
//////////////////////////////////////////////////////////////////////////////////
////
//// This code was developed as part of "Workshop on Multicore Algorithms"
//// at Tel-Aviv university, under the guidance of Prof. Nir Shavit and Moshe Sulamy.
////
//////////////////////////////////////////////////////////////////////////////////
//
//// This is one of eight implementations we created for the workshop.
//// In this implementation we used an improved fine grained locking method -
//// in every action that changes the data, we only locked the appropriate bucket.
//
//public class HopscotchHashTable{
//
//    //defs
//    final static int HOP_RANGE = 32;
//    final static int ADD_RANGE = 256;
//    final static int MAX_SEGMENTS = 1048576; // Including neighbourhood for last hash location
//
//    Bucket segments_arys[];		// The actual table
//    Locks locks;
//    int BUSY;
//
//    /*Bucket is the table object.
//    Each bucket contains a key and data pairing (as in a usual hashmap),
//    and an "hop_info" variable, containing the information
//    regarding all the keys that were initially mapped to the same bucket.*/
//    class Bucket {
//
//        volatile long _hop_info;
//        volatile int _key;
//        volatile int _data;
//
//        //CTR - bucket
//        Bucket() {
//            _hop_info = 0;
//            _key = -1;
//            _data = -1;
//        }
//    }
//
//    class Locks {
//        /*The number of locks in the table*/
//        int locks_size;
//        java.util.concurrent.locks.ReentrantLock lock_arys[];
//
//        //CTR - Locks
//        Locks(int locks_size) {
//            this.locks_size = locks_size;
//            lock_arys = new java.util.concurrent.locks.ReentrantLock[locks_size];
//            for(int i = 0; i < locks_size; i++) {
//                lock_arys[i] = new java.util.concurrent.locks.ReentrantLock();
//            }
//        }
//
//        /*Returns a lock index for a given table index.
//        Each lock is mapped to more than one table index (a.k.a bucket)
//        The mapping is done in the form of slices - so only
//        distant buckets will be mapped to the same lock*/
//        int get_index(int hash_index) {
//            return hash_index % locks_size;
//        }
//    }
//
//    //CTR - hopscotch
//    HopscotchHashTable(int locks_size) {
//        int size = MAX_SEGMENTS+256;
//        segments_arys = new Bucket[size];
//        for(int i = 0; i < size; i++) {
//            segments_arys[i] = new Bucket();
//        }
//        locks = new Locks(locks_size);
//        BUSY = -1;
//    }
//
//    //methods
//
//    /* void trial()
//    This is a method used for debugging purposes*/
//    void trial() {
//        Bucket temp;
//        int count = 0;
//        for(int i = 0; i < MAX_SEGMENTS+256; i++) {
//            temp = segments_arys[i];
//            if(temp._key != -1) {
//                count++;
//            }
//
//        }
//        System.out.println("Items in Hash = " + count);
//        System.out.println("--------------------");
//    }
//
//    /*int remove(int key)
//    Key - the key we'd like to remove from the table
//    Returns the data paired with key, if the table contained the key,
//    and NULL otherwise*/
//    int remove(int key) {
//        int hash = ((key) & (MAX_SEGMENTS-1));
//        Bucket start_bucket = segments_arys[hash];
//        // lock the lock in the locking system
//        locks.lock_arys[locks.get_index(hash)].lock();
//
//        long hop_info = start_bucket._hop_info;
//        long mask = 1;
//        for(int i = 0; i < HOP_RANGE; ++i, mask <<= 1) {
//            if((mask & hop_info) >= 1) {
//                Bucket check_bucket = segments_arys[hash+i];
//                if(key == check_bucket._key){
//                    int rc = check_bucket._data;
//                    check_bucket._key = -1;
//                    check_bucket._data = -1;
//                    start_bucket._hop_info &= ~(1<<i);
//                    locks.lock_arys[locks.get_index(hash)].unlock();
//                    return rc;
//                }
//            }
//        }
//        locks.lock_arys[locks.get_index(hash)].unlock();
//        return -1;
//    }
//
//    /*int[] find_closer_bucket(int free_bucket_index, int free_distance, int val)
//    free_bucket_index - the index of the first empty bucket in the table
//    (not in the neighbourhood)
//    free_distance - the function return a value via this var
//    val - the function return a value via this var
//
//    Return an array of return values -
//    0 - free distance, 1 - val, 2 - new free bucket
//    "free_distance" the distance between start_bucket and the newly freed bucket
//    Returns in val 0, if it was able to free a bucket in the neighbourhood of start_bucket,
//    otherwise, val remains unchanged
//    new_free_bucket the index of the newly freed bucket*/
//    int[] find_closer_bucket(int free_bucket_index,int free_distance,int val) {
//        //0 - free distance, 1 - val, 2 - new free bucket
//        int[] result = new int[3];
//        int move_bucket_index = free_bucket_index - (HOP_RANGE-1);
//        Bucket move_bucket = segments_arys[move_bucket_index];
//
//        for(int free_dist = (HOP_RANGE -1); free_dist > 0; --free_dist) {
//            long start_hop_info = move_bucket._hop_info;
//            int move_free_distance = -1;
//            long mask = 1;
//            for(int i = 0; i < free_dist; ++i, mask <<= 1) {
//                if((mask & start_hop_info) >= 1) {
//                    move_free_distance = i;
//                    break;
//                }
//            }
//			/*When a suitable bucket is found, it's content is moved to the old free_bucket*/
//            if(-1 != move_free_distance) {
//                locks.lock_arys[locks.get_index(move_bucket_index)].lock();
//                if(start_hop_info == move_bucket._hop_info) {
//                    int new_free_bucket_index = move_bucket_index + move_free_distance;
//                    Bucket new_free_bucket = segments_arys[new_free_bucket_index];
//					/*Updates move_bucket's hop_info, to indicate the newly inserted bucket*/
//                    move_bucket._hop_info |= (1 << free_dist);
//                    segments_arys[free_bucket_index]._data = new_free_bucket._data;
//                    segments_arys[free_bucket_index]._key = new_free_bucket._key;
//
//                    new_free_bucket._key = BUSY;
//                    new_free_bucket._data = BUSY;
//					/*Updates move_bucket's hop_info, to indicate the deleted bucket*/
//                    move_bucket._hop_info &= ~(1<<move_free_distance);
//
//                    free_distance = free_distance - free_dist + move_free_distance;
//                    locks.lock_arys[locks.get_index(move_bucket_index)].unlock();
//                    result[0] = free_distance;
//                    result[1] = val;
//                    result[2] = new_free_bucket_index;
//                    return result;
//                }
//                locks.lock_arys[locks.get_index(move_bucket_index)].unlock();
//            }
//            ++move_bucket_index;
//            move_bucket = segments_arys[move_bucket_index];
//        }
//        segments_arys[free_bucket_index]._key = -1;
//        result[0] = 0;
//        result[1] = 0;
//        result[2] = 0;
//        return result;
//    }
//
//    /* boolean contains(int key)
//    Key - the key we'd like to search for in the table
//    Returns true if the table contains the key, and false otherwise*/
//    boolean containsKey(int key) {
//        int hash = ((key)&(MAX_SEGMENTS-1));
//        Bucket start_bucket = segments_arys[hash];
//
//        long hop_info = start_bucket._hop_info;
//        int mask = 1;
//        for(int i = 0; i < HOP_RANGE; ++i, mask <<= 1) {
//            if((mask & hop_info) >= 1){
//                Bucket check_bucket = segments_arys[hash+i];
//                if(key == check_bucket._key) {
//                    return true;
//                }
//            }
//        }
//        return false;
//    }

//    int get(int key) {
//        int hash = ((key)&(MAX_SEGMENTS-1));
//        Bucket start_bucket = segments_arys[hash];
//
//        long hop_info = start_bucket._hop_info;
//        int mask = 1;
//        for(int i = 0; i < HOP_RANGE; ++i, mask <<= 1) {
//            if((mask & hop_info) >= 1){
//                Bucket check_bucket = segments_arys[hash+i];
//                if(key == check_bucket._key) {
//                    return check_bucket._data;
//                }
//            }
//        }
//        return -1;
//    }

    /* boolean add(int key, int data)
    Key, Data - the key and data pair we'd like to add to the table.
    Returns true if the operation was successful, and false otherwise*/
//    boolean put(int key,int data){
//        int val = 1;
//        int hash = ((key)&(MAX_SEGMENTS-1));
//        Bucket start_bucket = segments_arys[hash];
//        // lock the lock in the locking system
//        locks.lock_arys[locks.get_index(hash)].lock();
//
//        if(containsKey(key)) {
//            locks.lock_arys[locks.get_index(hash)].unlock();
//            return false;
//        }
//
//        int free_bucket_index = hash;
//        Bucket free_bucket = segments_arys[hash];
//        int free_distance = 0;
//        for(; free_distance < ADD_RANGE; ++free_distance) {
//            if(-1 == free_bucket._key) {
//                free_bucket._key = BUSY;
//                break;
//            }
//            ++free_bucket_index;
//            free_bucket = segments_arys[free_bucket_index];
//        }
//
//        //0 - free distance, 1 - val, 2 - new_free_bucket_index
//        int[] closest_bucket_info = new int[3];
//        if(free_distance < ADD_RANGE) {
//            do {
//                if(free_distance < HOP_RANGE) {
//					/*Inserts the new bucket to the free space*/
//                    start_bucket._hop_info |= (1<<free_distance);
//                    free_bucket._data = data;
//                    free_bucket._key = key;
//                    locks.lock_arys[locks.get_index(hash)].unlock();
//                    return true;
//                } else {
//					/*In case a free space was not found in the neighbourhood of start_bucket,
//					Clears such a space*/
//                    closest_bucket_info = find_closer_bucket(free_bucket_index, free_distance, val);
//
//                    free_distance = closest_bucket_info[0];
//                    val = closest_bucket_info[1];
//                    free_bucket_index = closest_bucket_info[2];
//                    free_bucket = segments_arys[free_bucket_index];
//                }
//            } while(0 != val);
//        }
//        locks.lock_arys[locks.get_index(hash)].unlock();
//        //System.out.println("Called Resize");
//        return false;
//    }
//
//}


import java.util.concurrent.locks.ReentrantLock;

public class HopscotchHashTable<K,V> implements Map<K,V> {
    //defs
    final static int HOP_RANGE = 32;
    final static int ADD_RANGE = 256;
    final static int MAX_SEGMENTS = 1048576; // Including neighbourhood for last hash location

    Bucket[] segments_arys;		// The actual table
    Locks locks;
    //K BUSY;

    /*Bucket is the table object.
    Each bucket contains a key and data pairing (as in a usual hashmap),
    and an "hop_info" variable, containing the information
    regarding all the keys that were initially mapped to the same bucket.*/
    class Bucket<K,V> {

        volatile long _hop_info;
        Entry<K,V> entry;
//        volatile int _key;
//        volatile int _data;

        //CTR - bucket
        Bucket() {
            _hop_info = 0;
            //entry = new Entry<K, V>(-1,-1,-1);
//            _key = -1;
//            _data = -1;
        }
    }

    class Locks {
        /*The number of locks in the table*/
        int locks_size;
        ReentrantLock lock_arys[];

        //CTR - Locks
        Locks(int locks_size) {
            this.locks_size = locks_size;
            lock_arys = new ReentrantLock[locks_size];
            for(int i = 0; i < locks_size; i++) {
                lock_arys[i] = new ReentrantLock();
            }
        }

        /*Returns a lock index for a given table index.
        Each lock is mapped to more than one table index (a.k.a bucket)
        The mapping is done in the form of slices - so only
        distant buckets will be mapped to the same lock*/
        int get_index(int hash_index) {
            return hash_index % locks_size;
        }
    }

    //CTR - hopscotch
    HopscotchHashTable() {
        int size = MAX_SEGMENTS+256;
        segments_arys = (Bucket<K,V>[]) new Object[size];
        for(int i = 0; i < size; i++) {
            segments_arys[i] = new Bucket();
        }
        locks = new Locks(size);
        //BUSY = -1;
    }

    //CTR - hopscotch
    HopscotchHashTable(int locks_size) {
        int size = MAX_SEGMENTS+256;
        for(int i=0;i<size;i++) {
            segments_arys[i] = new Bucket();
        }
        //segments_arys =  (Bucket[]) new Object[size];
        for(int i = 0; i < size; i++) {
            segments_arys[i] = new Bucket();
        }
        locks = new Locks(locks_size);
        //BUSY = -1;
    }

    //methods

    /* void trial()
    This is a method used for debugging purposes*/
    void trial() {
        Bucket temp;
        int count = 0;
        for(int i = 0; i < MAX_SEGMENTS+256; i++) {
            temp = segments_arys[i];
            if(temp.entry != null) {
                count++;
            }

        }
        System.out.println("Items in Hash = " + count);
        System.out.println("--------------------");
    }

    /*int remove(int key)
    Key - the key we'd like to remove from the table
    Returns the data paired with key, if the table contained the key,
    and NULL otherwise*/
    public V remove(K key) {
        int hash = ((key.hashCode()) & (MAX_SEGMENTS-1));
        Bucket start_bucket = segments_arys[hash];
        // lock the lock in the locking system
        locks.lock_arys[locks.get_index(hash)].lock();

        long hop_info = start_bucket._hop_info;
        long mask = 1;
        for(int i = 0; i < HOP_RANGE; ++i, mask <<= 1) {
            if((mask & hop_info) >= 1) {
                Bucket check_bucket = segments_arys[hash+i];
                if(key.equals(check_bucket.entry.key)){
                    V rc = (V) check_bucket.entry.value;
                    check_bucket.entry = null;
                    start_bucket._hop_info &= ~(1<<i);
                    locks.lock_arys[locks.get_index(hash)].unlock();
                    return rc;
                }
            }
        }
        locks.lock_arys[locks.get_index(hash)].unlock();
        return null;
    }

    /*int[] find_closer_bucket(int free_bucket_index, int free_distance, int val)
    free_bucket_index - the index of the first empty bucket in the table
    (not in the neighbourhood)
    free_distance - the function return a value via this var
    val - the function return a value via this var

    Return an array of return values -
    0 - free distance, 1 - val, 2 - new free bucket
    "free_distance" the distance between start_bucket and the newly freed bucket
    Returns in val 0, if it was able to free a bucket in the neighbourhood of start_bucket,
    otherwise, val remains unchanged
    new_free_bucket the index of the newly freed bucket*/
    int[] find_closer_bucket(int free_bucket_index,int free_distance,int val) {
        //0 - free distance, 1 - val, 2 - new free bucket
        int[] result = new int[3];
        int move_bucket_index = free_bucket_index - (HOP_RANGE-1);
        Bucket move_bucket = segments_arys[move_bucket_index];

        for(int free_dist = (HOP_RANGE -1); free_dist > 0; --free_dist) {
            long start_hop_info = move_bucket._hop_info;
            int move_free_distance = -1;
            long mask = 1;
            for(int i = 0; i < free_dist; ++i, mask <<= 1) {
                if((mask & start_hop_info) >= 1) {
                    move_free_distance = i;
                    break;
                }
            }
			/*When a suitable bucket is found, it's content is moved to the old free_bucket*/
            if(-1 != move_free_distance) {
                locks.lock_arys[locks.get_index(move_bucket_index)].lock();
                if(start_hop_info == move_bucket._hop_info) {
                    int new_free_bucket_index = move_bucket_index + move_free_distance;
                    Bucket new_free_bucket = segments_arys[new_free_bucket_index];
					/*Updates move_bucket's hop_info, to indicate the newly inserted bucket*/
                    move_bucket._hop_info |= (1 << free_dist);
                    segments_arys[free_bucket_index].entry.value = new_free_bucket.entry.value;
                    segments_arys[free_bucket_index].entry.key = new_free_bucket.entry.key;

                    new_free_bucket.entry = null;
					/*Updates move_bucket's hop_info, to indicate the deleted bucket*/
                    move_bucket._hop_info &= ~(1<<move_free_distance);

                    free_distance = free_distance - free_dist + move_free_distance;
                    locks.lock_arys[locks.get_index(move_bucket_index)].unlock();
                    result[0] = free_distance;
                    result[1] = val;
                    result[2] = new_free_bucket_index;
                    return result;
                }
                locks.lock_arys[locks.get_index(move_bucket_index)].unlock();
            }
            ++move_bucket_index;
            move_bucket = segments_arys[move_bucket_index];
        }
        segments_arys[free_bucket_index].entry = null;
        result[0] = 0;
        result[1] = 0;
        result[2] = 0;
        return result;
    }

    /* boolean contains(int key)
    Key - the key we'd like to search for in the table
    Returns true if the table contains the key, and false otherwise*/
    public boolean containsKey(K key) {
        int hash = ((key.hashCode())&(MAX_SEGMENTS-1));
        Bucket start_bucket = segments_arys[hash];

        long hop_info = start_bucket._hop_info;
        int mask = 1;
        for(int i = 0; i < HOP_RANGE; ++i, mask <<= 1) {
            if((mask & hop_info) >= 1){
                Bucket check_bucket = segments_arys[hash+i];
                if(key.equals(check_bucket.entry.key)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public V get(K key) {
        int hash = ((key.hashCode())&(MAX_SEGMENTS-1));
        Bucket start_bucket = segments_arys[hash];

        long hop_info = start_bucket._hop_info;
        int mask = 1;
        for(int i = 0; i < HOP_RANGE; ++i, mask <<= 1) {
            if((mask & hop_info) >= 1){
                Bucket check_bucket = segments_arys[hash+i];
                if(key.equals(check_bucket.entry.key)) {
                    V res = (V) check_bucket.entry.value;
                    return res;
                }
            }
        }
        return null;
    }

    //    public V containsAndSet(K key, V value) {
//        int hash = ((key.hashCode())&(MAX_SEGMENTS-1));
//        Bucket start_bucket = segments_arys[hash];
//
//        long hop_info = start_bucket._hop_info;
//        int mask = 1;
//        for(int i = 0; i < HOP_RANGE; ++i, mask <<= 1) {
//            if((mask & hop_info) >= 1){
//                Bucket check_bucket = segments_arys[hash+i];
//                if(key.equals(check_bucket.entry.key)) {
//                    V result = check_bucket.entry.value;
//                    if(value!=null)
//                        check_bucket.entry.value = value;
//                    return result;
//                }
//            }
//        }
//        return null;
//    }

    /* boolean add(int key, int data)
    Key, Data - the key and data pair we'd like to add to the table.
    Returns true if the operation was successful, and false otherwise*/
    public V put(K key, V value){
        int val = 1;
        int hash = ((key.hashCode())&(MAX_SEGMENTS-1));
        Bucket start_bucket = segments_arys[hash];
        // lock the lock in the locking system
        locks.lock_arys[locks.get_index(hash)].lock();

        // if key already exists, set the new value and return
        long hop_info = start_bucket._hop_info;
        long mask = 1;
        for(int i = 0; i < HOP_RANGE; ++i, mask <<= 1) {
            if((mask & hop_info) >= 1) {
                Bucket check_bucket = segments_arys[hash+i];
                if(key.equals(check_bucket.entry.key)){
                    V rc =  (V) check_bucket.entry.value;
                    check_bucket.entry.value = value;
                    start_bucket._hop_info &= ~(1<<i);
                    locks.lock_arys[locks.get_index(hash)].unlock();
                    return rc;
                }
            }
        }

        //if key doesn't exist
        int free_bucket_index = hash;
        Bucket free_bucket = segments_arys[hash];
        int free_distance = 0;
        for(; free_distance < ADD_RANGE; ++free_distance) {
            if(null == free_bucket.entry) {
                free_bucket.entry = new Entry<K, V>(key.hashCode(),key,value);
                break;
            }
            ++free_bucket_index;
            free_bucket = segments_arys[free_bucket_index];
        }

        //0 - free distance, 1 - val, 2 - new_free_bucket_index
        int[] closest_bucket_info = new int[3];
        if(free_distance < ADD_RANGE) {
            do {
                if(free_distance < HOP_RANGE) {
					/*Inserts the new bucket to the free space*/
                    start_bucket._hop_info |= (1<<free_distance);
                    free_bucket.entry = new Entry<K, V>(key.hashCode(),key,value);
                    locks.lock_arys[locks.get_index(hash)].unlock();
                    return null;
                } else {
					/*In case a free space was not found in the neighbourhood of start_bucket,
					Clears such a space*/
                    closest_bucket_info = find_closer_bucket(free_bucket_index, free_distance, val);

                    free_distance = closest_bucket_info[0];
                    val = closest_bucket_info[1];
                    free_bucket_index = closest_bucket_info[2];
                    free_bucket = segments_arys[free_bucket_index];
                }
            } while(0 != val);
        }
        locks.lock_arys[locks.get_index(hash)].unlock();
        resize();
        //System.out.println("Called Resize");
        return null;
    }

    protected void quiesce() {
        for (ReentrantLock lock : locks.lock_arys) {
            while (lock.isLocked()) {}  // spin
        }
    }

    public void resize() {
//        int oldCapacity = table.length;
//        int newCapacity = 2 * oldCapacity;
//        Thread me = Thread.currentThread();
//        if (owner.compareAndSet(null, me, false, true)) {
//            try {
//                if (table.length != oldCapacity) {  // someone else resized first
//                    return;
//                }
//                quiesce();
//                List<Entry<K,V>>[] oldTable = table;
//                table = (List<Entry<K,V>>[]) new List[newCapacity];
//                for (int i = 0; i < newCapacity; i++)
//                    table[i] = new ArrayList<Entry<K,V>>();
//                locks = new ReentrantLock[newCapacity];
//                for (int j = 0; j < locks.length; j++) {
//                    locks[j] = new ReentrantLock();
//                }
//                initializeFrom(oldTable);
//            } finally {
//                owner.set(null, false);       // restore prior state
//            }
//        }
    }

}