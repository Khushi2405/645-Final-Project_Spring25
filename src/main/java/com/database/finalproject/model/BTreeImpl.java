package com.database.finalproject.model;

import java.io.*;
import java.util.*;

public class BTreeImpl<K extends Comparable<K>, V> implements BTree<K, V> {
    private final int PAGE_SIZE = 4096;
    private RandomAccessFile indexFile;
    private BTreeNode<K> root;
    private int order;
    private int nextPageId = 0;

    public BTreeImpl(String indexFileName, int order) throws IOException {
        this.indexFile = new RandomAccessFile(indexFileName, "rw");
        this.order = order;
        this.root = new BTreeNode<>(true, order - 1);
    }

    @Override
    public void insert(K key, Rid rid) {
        // Insert logic with node splitting
    }

    @Override
    public Iterator<Rid> search(K key) {
        return null;
        // Search logic
        // return new ArrayList<>();
    }

    @Override
    public Iterator<Rid> rangeSearch(K startKey, K endKey) {
        return null;
        // Range search logic
        // return new ArrayList<>();
    }
}
