package HashTable;

import java.util.ArrayList;

/**
 * Created by Chunheng on 11/11, 2016
 * Associative Cuckoo Hash Table
 */
public abstract class NRECuckooHashTable<K,V> implements Map<K,V> {
    volatile int capacity;
    volatile ArrayList<TaggedEntry<K,V>>[][] table;
    // resize when overflow reaches this size
    static final int PROBE_SIZE = 8;
    static final int THRESHOLD = PROBE_SIZE / 2;
    // resize when chain of phases exceeds this
    static final int LIMIT = 8;

    private final static long multiplier = 0x5DEECE66DL;
    private final static long addend = 0xBL;
    private final static long mask = (1L << 31) - 1;

    /**
     * Create new set holding at least this many entries.
     * @param size number of entries to expect
     */
    @SuppressWarnings("unchecked")
	public NRECuckooHashTable(int size) {
        capacity = size;
        table = (ArrayList<TaggedEntry<K,V>>[][]) new ArrayList[2][capacity];
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < capacity; j++) {
                table[i][j] = new ArrayList<TaggedEntry<K,V>>(PROBE_SIZE);
            }
        }
    }

    final public int hash0(K key) {
        return (key.hashCode() & 0xffffff) % capacity;
    }

    final public synchronized int hash1(K key) {
        return (int) Math.abs((key.hashCode() * multiplier + addend) & mask);
    }

    public boolean containsKey(K key) {
        acquire(key);
        try {
            ArrayList<TaggedEntry<K,V>> set0 = table[0][hash0(key) % capacity];
            for(TaggedEntry<K,V> e: set0) {
                if (e.key.equals(key)) {
                    return true;
                }
            }
            ArrayList<TaggedEntry<K,V>> set1 = table[1][hash1(key) % capacity];
            for(TaggedEntry<K,V> e: set1) {
                if (e.key.equals(key)) {
                    return true;
                }
            }
//            if (table[0][h0].contains(key)) {
//                return true;
//            } else {
//                int h1 = hash1(key) % capacity;
//                if (table[1][h1].contains(key)) {
//                    return true;
//                }
//            }
            return false;
        } finally {
            release(key);
        }
    }

    public V remove(K key) {
        acquire(key);
        try {
            ArrayList<TaggedEntry<K,V>> set0 = table[0][hash0(key) % capacity];
            for(TaggedEntry<K,V> e: set0) {
                if (e.key.equals(key)) {
                    V result = e.value;
                    set0.remove(e);
                    return result;
                }
            }
            ArrayList<TaggedEntry<K,V>> set1 = table[1][hash1(key) % capacity];
            for(TaggedEntry<K,V> e: set1) {
                if (e.key.equals(key)) {
                    V result = e.value;
                    set1.remove(e);
                    return result;
                }
            }
            return null;
        } finally {
            release(key);
        }
    }

    public V get(K key) {
        acquire(key);
        try {
            ArrayList<TaggedEntry<K,V>> set0 = table[0][hash0(key) % capacity];
            for(TaggedEntry<K,V> e: set0) {
                if (e.key.equals(key)) {
                    V result = e.value;
                    return result;
                }
            }
            ArrayList<TaggedEntry<K,V>> set1 = table[1][hash1(key) % capacity];
            for(TaggedEntry<K,V> e: set1) {
                if (e.key.equals(key)) {
                    V result = e.value;
                    return result;
                }
            }
            return null;
        } finally {
            release(key);
        }
    }

    public V put(K key, V value) {
        acquire(key);
        int h0 = hash0(key) % capacity;
        int h1 = hash1(key) % capacity;
        int i = -1, h = -1;
        boolean mustResize = false;
        try {
            ArrayList<TaggedEntry<K,V>> set0 = table[0][h0];
            for(TaggedEntry<K,V> e: set0) {
                if (e.key.equals(key)) {
                    V result = e.value;
                    e.value = value;
                    return result;
                }
            }
            ArrayList<TaggedEntry<K,V>> set1 = table[1][h1];
            for(TaggedEntry<K,V> e: set1) {
                if (e.key.equals(key)) {
                    V result = e.value;
                    e.value = value;
                    return result;
                }
            }
            TaggedEntry<K,V> entry = new TaggedEntry<K, V>(key.hashCode(),key,value);
            if (set0.size() < THRESHOLD) {
                set0.add(entry);
                return null;
            } else if (set1.size() < THRESHOLD) {
                set1.add(entry);
                return null;
            } else if (set0.size() < PROBE_SIZE) {
                set0.add(entry);
                i = 0; h = h0;
            } else if (set1.size() < PROBE_SIZE) {
                set1.add(entry);
                i = 1; h = h1;
            } else {
                mustResize = true;
            }
        } finally {
            release(key);
        }
        if (mustResize) {
            resize();
            // System.out.println("Must Resize"); 
            put(key, value);
        } else if (!relocate(i, h)) {
        	// System.out.println("Relocate failed"); 
            resize();
        }
        return null;  // x must have been present
    }

    public abstract void acquire(K key);

    public abstract void release(K key);

    public abstract void resize();

    protected boolean relocate(int i, int hi) {
    	// System.out.println("Relocate called");
        int j = 1 - i;
        for (int round = 0; round < LIMIT; round++) {
            ArrayList<TaggedEntry<K,V>> iSet = table[i][hi];
            if (iSet.isEmpty()) return true; 
            /** 
             * Replacement policy: Not Relocated Entry (NRE)  
             */  
            int select_index = 0; 
            int hj = 0; 
            for (int entry_index = 0; entry_index < iSet.size(); entry_index++) {
            	TaggedEntry<K, V> entry_check = iSet.get(entry_index); 
            	int hj_check = 0; 
                switch (i) {
                	case 0: hj_check = hash1(entry_check.key) % capacity; break;
                	case 1: hj_check = hash0(entry_check.key) % capacity; break;
                }
                if (!(entry_check.relocated)) { 
                	hj = hj_check; 
                	select_index = entry_index; 
                	break; 
                }
            }
            TaggedEntry<K, V> entry = iSet.get(select_index); 
            entry.relocated = true; 
            K key = entry.key; 
            // 
            
            acquire(key);
            ArrayList<TaggedEntry<K,V>> jSet = table[j][hj];
            try {
                if (iSet.remove(entry)) {
                    if (jSet.size() < THRESHOLD) {
                        for (TaggedEntry<K,V> e : jSet) {
                        	e.relocated = false; 
                        }
                        jSet.add(entry); 
                        return true;
                    } else if (jSet.size() < PROBE_SIZE) {
                        for (TaggedEntry<K,V> e : jSet) {
                        	e.relocated = false; 
                        }
                        jSet.add(entry);
                        i = 1 - i;
                        hi = hj;
                        j = 1 - j;
                    } else {
                        iSet.add(entry);
                        return false;
                    }
                } else if (iSet.size() >= THRESHOLD) {
                    continue;
                } else {
                    return true;
                }
            } finally {
                release(key);
            }
        }
        return false;
    }

    public boolean check() {
        for (int i = 0; i < capacity; i++) {
            ArrayList<TaggedEntry<K,V>> set = table[0][i];
            for (TaggedEntry<K,V> e: set) {
                if ((hash0(e.key) % capacity) != i) {
                    System.out.printf("Unexpected value %d at table[0][%d] hash %d\n",
                            e, i, hash0(e.key) % capacity);
                    return false;
                }
            }
        }
        for (int i = 0; i < capacity; i++) {
            ArrayList<TaggedEntry<K,V>> set = table[1][i];
            for (TaggedEntry<K,V> e: set) {
                if ((hash1(e.key) % capacity) != i) {
                    System.out.printf("Unexpected value %d at table[0][%d] hash %d\n",
                            e, i, hash1(e.key) % capacity);
                    return false;
                }
            }
        }
        return true;
    }

//    private boolean present(K key) {
//        ArrayList<TaggedEntry<K,V>> set0 = table[0][hash0(key) % capacity];
//        for(TaggedEntry<K,V> e: set0) {
//            if (e.key.equals(key)) {
//                return true;
//            }
//        }
//        ArrayList<TaggedEntry<K,V>> set1 = table[1][hash1(key) % capacity];
//        for(TaggedEntry<K,V> e: set1) {
//            if (e.key.equals(key)) {
//                return true;
//            }
//        }
//        return false;
//    }

    public boolean check(int expectedSize) {
        int size = 0;
        for (int i = 0; i < capacity; i++) {
            for (TaggedEntry<K,V> entry : table[0][i]) {
                if (entry != null) {
                    size++;
                    if ((hash0(entry.key) % capacity) != i) {
                        System.out.printf("Unexpected value %d at table[0][%d] hash %d\n",
                                entry, i, hash0(entry.key) % capacity);
                        return false;
                    }
                }
            }
            for (TaggedEntry<K,V> e : table[1][i]) {
                if (e != null) {
                    size++;
                    if ((hash1(e.key) % capacity) != i) {
                        System.out.printf("Unexpected value %d at table[0][%d] hash %d\n",
                                e, i, hash1(e.key) % capacity);
                        return false;
                    }
                }
            }
        }
        if (size != expectedSize) {
            System.out.printf("Bad size: found %d, expected %d\n", size, expectedSize);
            return false;
        }
        return true;
    }
}
