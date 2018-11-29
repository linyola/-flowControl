/*
 *   Author: mario.zwk.
 *   Modifier:
 *   Date: 2018/11/29
 *   限流窗口
 */

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.TreeMap;


public class SpeedLimit<TKey> {
    private int high;
    private int window;

    private final SortedMap<TKey, Record<TKey>> smap;
    private final PriorityQueue<Record<TKey>> priorQuque = new PriorityQueue<SpeedLimit<TKey>.Record<TKey>>(11, new RecordCompare<TKey>());

    public SpeedLimit(Comparator<? super TKey> comparator) {
        smap = new TreeMap<TKey, SpeedLimit<TKey>.Record<TKey>>(comparator);
    }

    public void config(int window,int high) {
        this.high = high;
        this.window	= window;
    }

    public boolean add(TKey key) {
        long now = System.currentTimeMillis() / 1000;
        while(!priorQuque.isEmpty() && priorQuque.peek().isTimeout(now)) {
            TKey i = priorQuque.peek().key;
            priorQuque.poll();
            smap.remove(i);
        }
        Record<TKey> record = smap.get(key);
        if(null != record) {
            return record.update(high);
        }

        Record<TKey> rd = new Record<TKey>(key,window);
        priorQuque.offer(rd);
        smap.put(key, rd);
        return true;
    }

    public boolean ask(TKey key) {
        long now = System.currentTimeMillis() / 1000;
        while(!priorQuque.isEmpty() && priorQuque.peek().isTimeout(now)) {
            TKey i = priorQuque.peek().key;
            priorQuque.poll();
            smap.remove(i);
        }
        Record<TKey> record = smap.get(key);
        if(null != record) {
            return record.isLimit(high);
        }
        return true;
    }

    public void clear(TKey key) {
        smap.remove(key);
    }

    public void dump() {
        while (!priorQuque.isEmpty()){
            TKey i = priorQuque.poll().key;
            System.out.print(i + " ");
        }
        System.out.println();
    }

    class Record <K> {
        K key;
        int count = 1;
        long deadline;

        Record(K key, int window) {
            this.key = key;
            deadline = window + System.currentTimeMillis() / 1000;
        }

        boolean isTimeout(long deadline) {
            return this.deadline <= deadline;
        }
        boolean update(int high) {
            return count++ < high;
        }
        boolean isLimit(int high) {
            return count < high;
        }
    }

    class RecordCompare<K> implements Comparator<Record<K>>{

        public int compare(Record<K> o1, Record<K> o2) {
            return o1.deadline > o2.deadline ? -1 : (o1.deadline == o2.deadline ? 0 : 1);
        }

    }

}

