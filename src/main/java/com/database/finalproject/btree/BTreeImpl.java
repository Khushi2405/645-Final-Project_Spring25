package com.database.finalproject.btree;

import java.util.Iterator;
import java.util.Arrays;

public class BTreeImpl<K extends Comparable<K>, V> {

    public static int max_num_nodes = 4;

    boolean isLeaf;
    int num_nodes;
    K[] keys;
    BTreeImpl<K,V> parent;
    /*
     * Pointers explanation:
     * If non-leaf node:
     *      each of child nodes (if extra, then -1)
     * If child node:
     *      all set to -1
     */
    int[] pointers;
    V[] recordIds;
    BTreeImpl<K,V> next;

    @SuppressWarnings("unchecked")
    public BTreeImpl() {
        this.isLeaf = true;
        this.num_nodes = 0;
        keys = (K[]) new Comparable[max_num_nodes];
        pointers = new int[max_num_nodes + 1];
        Arrays.fill(pointers,-1);
    }

    @SuppressWarnings("unchecked")
    public BTreeImpl(BTreeImpl<K,V> parent, BTreeImpl<K,V> next) {
        this.isLeaf = true;
        this.num_nodes = 0;
        keys = (K[]) new Comparable[max_num_nodes];
        pointers = new int[max_num_nodes + 1];
        Arrays.fill(pointers,-1);
        this.parent = parent;
        this.next = next;
        this.recordIds = (V[]) new Comparable[max_num_nodes];
    }

    public void insert(K key, V rid) {
        if (this.isLeaf) {
            boolean space = false;
            for (int i = 1; i < keys.length; i++) {
                if (keys[i] == null) {
                    space = true;
                    break;
                }
            }
            if (space) {
                insertSorted(key, rid);
            }
            else {
                this.split(key, rid);
            }
        }
        else {
            /*
             * TODO
             */
        }
    }

    private void insertSorted(K key, V rid) {
        K[] arr = this.keys;
        V[] recordArr = this.recordIds;
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] != null) {
                if (arr[i].compareTo(key) > 0) {
                    K temp = arr[i];
                    arr[i] = key;
                    key = temp;
                    V tempR = recordArr[i];
                    recordArr[i] = rid;
                    rid = tempR;
                }
            }
            else {
                arr[i] = key;
                break;
            }
        }
        this.keys = arr;
    }

    @SuppressWarnings("unchecked")
    public void split(K key, V rid) {
        /*
         * TODO: Fix middle_node_index
         */
        int middle_node_index = (max_num_nodes + 1) / 2;
        K middle_node_key = this.keys[middle_node_index];
        BTreeImpl<K,V> newNode = new BTreeImpl<>(this.parent, this.next);
        this.next = newNode;
        K[] newKeys = (K[]) new Comparable[max_num_nodes];
        V[] newRids = (V[]) new Comparable[max_num_nodes];
        int[] newPointers = new int[max_num_nodes + 1];
        int starting_index = middle_node_index;
        if (!this.isLeaf) {
            starting_index++;
        }
        for (int i = starting_index; i < max_num_nodes; i++) {
            newKeys[i - starting_index] = this.keys[i];
            this.keys[i] = null;
            if (this.isLeaf) {
                newRids[i - starting_index] = this.recordIds[i];
                this.recordIds[i] = null;
            }
            else {
                /*
                 * TODO
                 */
            }
        }
        newNode.setKeys(newKeys);
        if (this.isLeaf) {
            newNode.setRids(newRids);
        }
        else {
            newNode.setPointers(newPointers);
        }
    }

    public K[] insertSortedFull(K val, V rid) {
        K[] arr = this.keys;
        K[] newArr = (K[]) new Comparable[arr.length + 1];
        boolean doneVal = false;
        int i = 0;
        int j = 0;
        while (i < arr.length || !doneVal) {
            if (i == arr.length) {
                newArr[j] = val;
                doneVal = true;
            }
            else if (!doneVal && val < arr[i]) {
                newArr[j] = val;
                doneVal = true;
            }
            else {
                newArr[j] = arr[i];
                i++;
            }
            j++;
        }
        return newArr;
    }

    public void setKeys(K[] arr) {
        this.keys = arr;
    }

    public void setRids(V[] arr) {
        this.recordIds = arr;
    }

    public void setPointers(int[] arr) {
        this.pointers = arr;
    }

    public Iterator<V> search(K key) {

    }

    public Iterator<V> rangeSearch(K startKey, K endKey) {

    }
}
