package com.database.finalproject.btree;

import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.model.IndexPage;
import com.database.finalproject.model.Rid;

import java.nio.ByteBuffer;
import java.util.*;

import static com.database.finalproject.constants.PageConstants.*;

public class BTreeImpl implements BTree<String, Rid> {

    BufferManager bf;
    int rootPageId;
    int catalogIndex;

    public BTreeImpl(BufferManager bf, int catalogIndex) {
        this.catalogIndex = catalogIndex;
        this.bf = bf;
        rootPageId = Integer.parseInt(bf.getRootPageId(catalogIndex));
        if(rootPageId == -1) {
            IndexPage rootPage = (IndexPage) bf.createPage(catalogIndex);
            rootPage.setIsLeaf(true);
            rootPageId = rootPage.getPid();
            bf.setRootPageId(rootPageId, catalogIndex);
            bf.unpinPage(rootPageId, catalogIndex);
        }

    }

    @Override
    public void insert(String key, Rid rid) {
        IndexPage leaf = findLeaf(key);
        insertIntoLeaf(leaf, key, rid.getPageId(), rid.getSlotId());
        if (leaf.keys.size() >= leaf.getOrder()) {
            splitLeafNode(leaf);
        }
        bf.markDirty(leaf.getPid(), catalogIndex);
        bf.unpinPage(leaf.getPid(), catalogIndex);
    }

    private IndexPage findLeaf(String key) {
        IndexPage nodePage = loadRootPage(rootPageId);
        while (!nodePage.getIsLeaf()) {
            int i = 0;
            while (i < nodePage.keys.size()
                    && key.compareTo(new String(removeTrailingBytes(nodePage.keys.get(i))).trim()) >= 0) {
                i++;
            }
            bf.unpinPage(nodePage.getPid(), catalogIndex);
            nodePage = (IndexPage) bf.getPage(bytesToInt(nodePage.pageIds.get(i)), catalogIndex);
        }
        return nodePage;
    }

    private void insertIntoLeaf(IndexPage leafPage, String key, int pageId, int slotId) {
        int i = 0;
        while (i < leafPage.keys.size()
                && key.compareTo(new String(removeTrailingBytes(leafPage.keys.get(i))).trim()) >= 0) {
            i++;
        }
        leafPage.keys.add(i, truncateOrPadByteArray(key.getBytes(), catalogIndex == MOVIE_ID_INDEX_PAGE_INDEX ? MOVIE_ID_SIZE : MOVIE_TITLE_SIZE));
        leafPage.pageIds.add(i, intToBytes(pageId, PAGE_ID_SIZE));
        leafPage.slotIds.add(i, intToBytes(slotId, SLOT_ID_SIZE));
    }

    private void splitLeafNode(IndexPage leafPage) {
        int mid = leafPage.keys.size() / 2;
        IndexPage newLeafPage = (IndexPage) bf.createPage(catalogIndex);
        newLeafPage.setIsLeaf(true);
        newLeafPage.keys.addAll(leafPage.keys.subList(mid, leafPage.keys.size()));
        newLeafPage.pageIds.addAll(leafPage.pageIds.subList(mid, leafPage.pageIds.size()));
        newLeafPage.slotIds.addAll(leafPage.slotIds.subList(mid, leafPage.slotIds.size()));

        leafPage.keys.subList(mid, leafPage.keys.size()).clear();
        leafPage.pageIds.subList(mid, leafPage.pageIds.size()).clear();
        leafPage.slotIds.subList(mid, leafPage.slotIds.size()).clear();

        int nextLeafId = bytesToInt(leafPage.nextLeaf);
        if (nextLeafId != -1) {
            IndexPage nextLeafPage = (IndexPage) bf.getPage(nextLeafId, catalogIndex);
            newLeafPage.nextLeaf = intToBytes(nextLeafPage.getPid(), PAGE_ID_SIZE);
            nextLeafPage.prevLeaf = intToBytes(newLeafPage.getPid(), PAGE_ID_SIZE);

            bf.markDirty(nextLeafPage.getPid(), catalogIndex);
            bf.unpinPage(nextLeafPage.getPid(), catalogIndex);
        }
        leafPage.nextLeaf = intToBytes(newLeafPage.getPid(), PAGE_ID_SIZE);
        newLeafPage.prevLeaf = intToBytes(leafPage.getPid(), PAGE_ID_SIZE);

        // mark dirty and unpin leaf nodes
        bf.unpinPage(newLeafPage.getPid(), catalogIndex);

        insertIntoParent(leafPage, newLeafPage.keys.get(0), newLeafPage);
    }

    private void insertIntoParent(IndexPage leftPage, byte[] key, IndexPage rightPage) {
        if (leftPage.getPid() == rootPageId) {
            IndexPage newRootPage = (IndexPage) bf.createPage(catalogIndex);
            newRootPage.setIsLeaf(false);
            newRootPage.keys.add(key);
            newRootPage.pageIds.add(intToBytes(leftPage.getPid(), PAGE_ID_SIZE));

            newRootPage.pageIds.add(intToBytes(rightPage.getPid(), PAGE_ID_SIZE));
            rootPageId = newRootPage.getPid();
            bf.setRootPageId(rootPageId, catalogIndex);

            bf.unpinPage(newRootPage.getPid(), catalogIndex);
            return;
        }

        IndexPage rootPage = loadRootPage(rootPageId);
        IndexPage parent = findParent(rootPage, leftPage);

        if(rootPageId != parent.getPid())
            bf.unpinPage(rootPageId, catalogIndex);

        int insertIdx = 0;
        while (insertIdx < parent.pageIds.size() && bytesToInt(parent.pageIds.get(insertIdx)) != leftPage.getPid()) {
            insertIdx++;
        }

        parent.keys.add(insertIdx, key);
        parent.pageIds.add(insertIdx + 1, intToBytes(rightPage.getPid(), PAGE_ID_SIZE));
        if (parent.keys.size() >= (catalogIndex == MOVIE_ID_INDEX_PAGE_INDEX  ? MOVIE_ID_NON_LEAF_NODE_ORDER : MOVIE_TITLE_NON_LEAF_NODE_ORDER)){
            splitInternalNode(parent);
        }
        bf.markDirty(parent.getPid(), catalogIndex);
        bf.unpinPage(parent.getPid(), catalogIndex);

    }

    private IndexPage findParent(IndexPage node, IndexPage child) {
        if (node == null || node.getIsLeaf()) {
            return null;
        }

        // Check if this node is the direct parent
        for (int i = 0; i < node.pageIds.size(); i++) {
            int pageId = bytesToInt(node.pageIds.get(i));
            if (pageId == child.getPid()) {
                return node;
            }
        }

        // Recursively check all children
        IndexPage result = null;
        for (int i = 0; i < node.pageIds.size(); i++) {
            int pageId = bytesToInt(node.pageIds.get(i));
            IndexPage subChild = (IndexPage) bf.getPage(pageId, catalogIndex);

            if (subChild != null) {
                result = findParent(subChild, child);
                if (result != null) {
                    return result;
                }
                bf.unpinPage(subChild.getPid(), catalogIndex);
            }
        }

        return null;
    }

    private void splitInternalNode(IndexPage node) {
        int mid = node.keys.size() / 2;
        IndexPage newInternal = (IndexPage) bf.createPage(catalogIndex);
        newInternal.setIsLeaf(false);

        newInternal.keys.addAll(node.keys.subList(mid + 1, node.keys.size()));
        newInternal.pageIds.addAll(node.pageIds.subList(mid + 1, node.pageIds.size()));

        byte[] promotedKey = node.keys.get(mid);
        node.keys.subList(mid, node.keys.size()).clear();
        node.pageIds.subList(mid + 1, node.pageIds.size()).clear();

        if (node.getPid() == rootPageId) {
            IndexPage newRoot = (IndexPage) bf.createPage(catalogIndex);
            newRoot.setIsLeaf(false);
            newRoot.keys.add(promotedKey);
            newRoot.pageIds.add(intToBytes(node.getPid(), PAGE_ID_SIZE));
            newRoot.pageIds.add(intToBytes(newInternal.getPid(), PAGE_ID_SIZE));
            rootPageId = newRoot.getPid();
            bf.setRootPageId(rootPageId, catalogIndex);
            bf.unpinPage(newRoot.getPid(), catalogIndex);
        } else {
            insertIntoParent(node, promotedKey, newInternal);
        }

        // mark dirty and unpin
        bf.unpinPage(newInternal.getPid(), catalogIndex);
    }

    @Override
    public Iterator<Rid> search(String key) {
        IndexPage leaf = findLeaf(key);
        bf.unpinPage(leaf.getPid(), catalogIndex);

        int index = binarySearch(leaf.keys, key);
        if (index >= 0) {
            List<Rid> result = new ArrayList<>();
            while (index < leaf.keys.size()
                    && key.compareTo(new String(removeTrailingBytes(leaf.keys.get(index))).trim()) == 0) {
                result.add(new Rid(bytesToInt(leaf.pageIds.get(index)), bytesToInt(leaf.slotIds.get(index))));
                index++;
            }
            return result.iterator();
        }
        return Collections.emptyIterator();
    }

    private int binarySearch(List<byte[]> keys, String target) {
        int low = 0, high = keys.size() - 1;
        while (low <= high) {
            int mid = (low + high) / 2;
            int cmp = new String(removeTrailingBytes(keys.get(mid))).trim().compareTo(target);
            if (cmp == 0) return mid;
            if (cmp < 0) low = mid + 1;
            else high = mid - 1;
        }
        return -1;
    }

    @Override
    public Iterator<Rid> rangeSearch(String startKey, String endKey) {
        IndexPage leaf = findLeaf(startKey);
        List<Rid> result = new ArrayList<>();

        while (leaf != null) {
            for (int i = 0; i < leaf.keys.size(); i++) {
                String currentKey = new String(removeTrailingBytes(leaf.keys.get(i))).trim();
                // Check if key is within range (startKey <= currentKey <= endKey)
                if (currentKey.compareTo(startKey) >= 0 && currentKey.compareTo(endKey) <= 0) {
                    result.add(new Rid(bytesToInt(leaf.pageIds.get(i)), bytesToInt(leaf.slotIds.get(i))));
                } else if (currentKey.compareTo(endKey) > 0) {
                    // If we've passed the end key, stop searching
                    bf.unpinPage(leaf.getPid(), catalogIndex);
                    return result.iterator();
                }
            }
            bf.unpinPage(leaf.getPid(), catalogIndex);
            leaf = (IndexPage) bf.getPage(bytesToInt(leaf.nextLeaf),catalogIndex);
        }

        return result.iterator();
    }

    private IndexPage loadRootPage(int rootPageId){
        return (IndexPage) bf.getPage(rootPageId, catalogIndex);
    }

    public long printKeys() {
        IndexPage nodePage = loadRootPage(rootPageId);
        while (!nodePage.getIsLeaf()) {
            bf.unpinPage(nodePage.getPid(), catalogIndex);
            nodePage = (IndexPage) bf.getPage(bytesToInt(nodePage.pageIds.get(0)), catalogIndex);
        }
        String prev = "";
        long countRecords = 0;
        while (nodePage != null) {
            for (byte[] key : nodePage.keys) {
                countRecords++;
                String curr = new String(removeTrailingBytes(key)).trim();
                if (prev.compareTo(curr) > 0) {
                    System.out.println(prev + " " + curr);
                }
                prev = curr;
            }

            // Move to the next leaf node
            if (bytesToInt(nodePage.nextLeaf) != -1) {
                bf.unpinPage(nodePage.getPid(), catalogIndex);
                nodePage = (IndexPage) bf.getPage(bytesToInt(nodePage.nextLeaf), catalogIndex);
            } else {
                break;
            }
        }
        System.out.println(countRecords);
        return countRecords;
    }



}
