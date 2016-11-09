import java.util.Iterator;
import java.util.concurrent.atomic.AtomicMarkableReference;

/**
 * Created by Liheng on 11/7/16.
 */
public class SplitOrderedList<K,V> implements Map<K,V> {
    static final int WORD_SIZE = 24;
    static final int LO_MASK = 0x00000001;
    static final int HI_MASK = 0x00800000;
    static final int MASK = 0x00FFFFFF;
    Node head;
    /**
     * Constructor
     */
    public SplitOrderedList() {
        this.head = new Node(0);
        this.head.next =
                new AtomicMarkableReference<Node>(new Node(Integer.MAX_VALUE), false);
    }
    private SplitOrderedList(Node e) {
        this.head  = e;
    }
    /**
     * Restricted-size hash code
     * @param x object to hash
     * @return hash code
     */
    public static int hashCode(Object x) {
        return x.hashCode() & MASK;
    }
    public V put(K key, V value) {
        if(value==null) {
            throw new NullPointerException();
        }
        int index = makeRegularIndex(key);
        boolean splice;
        while (true) {
            // find predecessor and current entries
            Window window = find(head, index);
            Node pred = window.pred;
            Node curr = window.curr;
            // is the key present?
            if (curr.index == index) {
                V oldValue = curr.entry.value;
                curr.entry.value = value;
                return oldValue;
            } else {
                // splice in new entry
                Node node = new Node(index, new Entry<K, V>(key.hashCode(), key, value));
                node.next.set(curr, false);
                splice = pred.next.compareAndSet(curr, node, false, false);
                if (splice)
                    return null;
                else
                    continue;
            }
        }
    }

    @Override
    public V get(K key) {
        int index = makeRegularIndex(key);
        while (true) {
            // find predecessor and current entries
            Window window = find(head, index);
            Node pred = window.pred;
            Node curr = window.curr;
            // is the key present?
            if (curr.index != index) {
                return null;
            } else {
                // snip out matching entry
                return curr.entry.value;
            }
        }
    }

    public V remove(K key) {
        int index = makeRegularIndex(key);
        boolean snip;
        while (true) {
            // find predecessor and current entries
            Window window = find(head, index);
            Node pred = window.pred;
            Node curr = window.curr;
            // is the key present?
            if (curr.index != index) {
                return null;
            } else {
                // snip out matching entry
                V result = curr.entry.value;
                snip = pred.next.attemptMark(curr, true);
                if (snip)
                    return result;
                else
                    continue;
            }
        }
    }
    public boolean containsKey(K key) {
        int index = makeRegularIndex(key);
        Window window = find(head, index);
        Node pred = window.pred;
        Node curr = window.curr;
        return (curr.index == index);
    }

    public SplitOrderedList<K,V> getSentinel(int ind) {
        int index = makeSentinelKey(ind);
        boolean splice;
        while (true) {
            // find predecessor and current entries
            Window window = find(head, index);
            Node pred = window.pred;
            Node curr = window.curr;
            // is the key present?
            if (curr.index == index) {
                return new SplitOrderedList<K, V>(curr);
            } else {
                // splice in new entry
                Node node = new Node(index);
                node.next.set(pred.next.getReference(), false);
                splice = pred.next.compareAndSet(curr, node, false, false);
                if (splice)
                    return new SplitOrderedList<K, V>(node);
                else
                    continue;
            }
        }
    }

    public static int reverse(int key) {
        int loMask = LO_MASK;
        int hiMask = HI_MASK;
        int result = 0;
        for (int i = 0; i < WORD_SIZE; i++) {
            if ((key & loMask) != 0) {  // bit set
                result |= hiMask;
            }
            loMask <<= 1;
            hiMask >>>= 1;  // fill with 0 from left
        }
        return result;
    }
    public int makeRegularIndex(K key) {
        int code = key.hashCode() & MASK; // take 3 lowest bytes
        return reverse(code | HI_MASK);
    }
    private int makeSentinelKey(int index) {
        return reverse(index & MASK);
    }

    private class Node {
        int index;
        Entry<K,V> entry;
        AtomicMarkableReference<Node> next;
        Node(int index, Entry<K,V> entry) {      // usual constructor
            this.index = index;
            this.entry = entry;
            this.next  = new AtomicMarkableReference<Node>(null, false);
        }
        Node(int index) { // sentinel constructor
            this.index  = index;
            this.next = new AtomicMarkableReference<Node>(null, false);
        }

        Node getNext() {
            boolean[] cMarked = {false}; // is curr marked?
            boolean[] sMarked = {false}; // is succ marked?
            Node node = this.next.get(cMarked);
            while (cMarked[0]) {
                Node succ = node.next.get(sMarked);
                this.next.compareAndSet(node, succ, true, sMarked[0]);
                node = this.next.get(cMarked);
            }
            return node;
        }
    }
    class Window {
        public Node pred;
        public Node curr;
        Window(Node pred, Node curr) {
            this.pred = pred;
            this.curr = curr;
        }
    }
    public Window find(Node head, int index) {
        Node pred = head;
        Node curr = head.getNext();
        while (curr.index < index) {
            pred = curr;
            curr = pred.getNext();
        }
        return new Window(pred, curr);
    }
}
