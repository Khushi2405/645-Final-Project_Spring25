package com.database.finalproject.model;

import java.io.*;
import java.util.*;
import java.io.Serializable;

// B+ Tree Node
public class BTreeNode<K extends Comparable<K>> implements Serializable {
    public boolean isLeaf;
    public List<K> keys; // Array of keys in node
    public List<Object> children; // Rids for leaf nodes, page pointers for internal nodes
    public int numKeys;

    public BTreeNode(boolean isLeaf, int numKeys) {
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>();
        this.children = new ArrayList<>();
        this.numKeys = numKeys;
    }

    public boolean isFull() {
        // TODO: isFull logic
        return false;
    }
}

// class BTreeNode<K extends Comparable<K>, V> implements Serializable {
// private static final long serialVersionUID = 1L;

// int maxKeys; // Maximum keys per node
// int numKeys; // Current number of keys
// boolean isLeaf;

// List<K> keys;
// List<Long> childrenOffsets; // File offsets of child nodes
// List<V> values; // Values for leaf nodes

// public BTreeNode(int maxKeys, boolean isLeaf) {
// this.maxKeys = maxKeys;
// this.numKeys = 0;
// this.isLeaf = isLeaf;
// this.keys = new ArrayList<>();
// this.childrenOffsets = new ArrayList<>();
// this.values = new ArrayList<>();
// }

// // Serialize Node to a Byte Array
// public byte[] serialize() throws IOException {
// ByteArrayOutputStream bos = new ByteArrayOutputStream();
// ObjectOutputStream oos = new ObjectOutputStream(bos);
// oos.writeObject(this);
// oos.flush();
// return bos.toByteArray();
// }

// // Deserialize Node from a Byte Array
// public static <K extends Comparable<K>, V> BTreeNode<K, V> deserialize(byte[]
// data)
// throws IOException, ClassNotFoundException {
// ByteArrayInputStream bis = new ByteArrayInputStream(data);
// ObjectInputStream ois = new ObjectInputStream(bis);
// return (BTreeNode<K, V>) ois.readObject();
// }
// }