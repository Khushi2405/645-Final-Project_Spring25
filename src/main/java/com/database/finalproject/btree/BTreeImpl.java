package com.database.finalproject.btree;

import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.model.IndexPage;
import com.database.finalproject.model.Rid;

import java.nio.ByteBuffer;
import java.util.*;

import static com.database.finalproject.constants.PageConstants.*;

public class BTreeImpl implements BTree<String, Rid> {
    // TODO: Btree-id changes?

    BufferManager bf;
    int rootPageId;
    // IndexPage rootPage;
    int catalogIndex;

    public BTreeImpl(BufferManager bf, int catalogIndex) {
        this.catalogIndex = catalogIndex;
        this.bf = bf;
        rootPageId = Integer.parseInt(bf.getRootPageId(catalogIndex));
        // System.out.println("In constructor of Btree impl: " + rootPageId);
        if (rootPageId == -1) {
            // System.out.println("New root created");
            IndexPage rootPage = (IndexPage) bf.createPage(catalogIndex);
            rootPage.setIsLeaf(true);
            rootPageId = rootPage.getPid();
            bf.setRootPageId(rootPageId, catalogIndex);
            bf.unpinPage(rootPageId, catalogIndex);
        }
        // System.out.println(rootPageId);
        // if(rootPage == null) rootPage = (IndexPage) bf.createPage(catalogIndex);

    }

    @Override
    public void insert(String key, Rid rid) {
        IndexPage leaf = findLeaf(key);
        insertIntoLeaf(leaf, key, rid.getPageId(), rid.getSlotId());
        if (leaf.keys.size() >= leaf.getOrder()) {
            System.out.println("Is leaf root: " + (leaf.getPid() == rootPageId));
            splitLeafNode(leaf);
        }
        bf.markDirty(leaf.getPid(), catalogIndex);
        bf.unpinPage(leaf.getPid(), catalogIndex);
    }

    private IndexPage findLeaf(String key) {
        IndexPage nodePage = loadRootPage(rootPageId);
        // System.out.println("Root page is: " + nodePage);
        while (!nodePage.getIsLeaf()) {
            bf.unpinPage(nodePage.getPid(), catalogIndex);
            int i = 0;
            // TODO greater than 0 or less than ?
            while (i < nodePage.keys.size()
                    && key.compareTo(new String(removeTrailingBytes(nodePage.keys.get(i)))) < 0) {
                i++;
            }
            nodePage = (IndexPage) bf.getPage(bytesToInt(nodePage.pageIds.get(i)), catalogIndex);
        }
        return nodePage;
    }

    private void insertIntoLeaf(IndexPage leafPage, String key, int pageId, int slotId) {
        int i = 0;
        while (i < leafPage.keys.size()
                && key.compareTo(new String(removeTrailingBytes(leafPage.keys.get(i)))) > 0) {
            i++;
        }
        leafPage.keys.add(i, truncateOrPadByteArray(key.getBytes(),
                catalogIndex == MOVIE_ID_INDEX_PAGE_INDEX ? MOVIE_ID_SIZE : MOVIE_TITLE_SIZE));
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
        int insertIdx = 0;
        while (insertIdx < parent.pageIds.size() && bytesToInt(parent.pageIds.get(insertIdx)) != leftPage.getPid()) {
            insertIdx++;
        }
        bf.unpinPage(rootPageId, catalogIndex);

        parent.keys.add(insertIdx, key);
        parent.pageIds.add(insertIdx + 1, intToBytes(rightPage.getPid(), PAGE_ID_SIZE));
        if (parent.keys.size() >= (catalogIndex == MOVIE_ID_INDEX_PAGE_INDEX ? MOVIE_ID_NON_LEAF_NODE_ORDER
                : MOVIE_TITLE_NON_LEAF_NODE_ORDER)) {
            splitInternalNode(parent);
        }
        bf.markDirty(parent.getPid(), catalogIndex);
        bf.unpinPage(parent.getPid(), catalogIndex);
    }

    private IndexPage findParent(IndexPage node, IndexPage child) {
        if (node.getIsLeaf())
            return null;
        for (byte[] n : node.pageIds) {
            int subChildPageId = bytesToInt(n);
            if (subChildPageId == child.getPid())
                return node;
            IndexPage subChild = (IndexPage) bf.getPage(subChildPageId, catalogIndex);
            IndexPage parent = findParent(subChild, child);
            if (parent != null)
                return parent;
            bf.unpinPage(subChildPageId, catalogIndex);
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
            newRoot.keys.add(promotedKey);
            newRoot.pageIds.add(intToBytes(node.getPid(), PAGE_ID_SIZE));
            newRoot.pageIds.add(intToBytes(newInternal.getPid(), PAGE_ID_SIZE));
            rootPageId = newRoot.getPid();
            bf.setRootPageId(rootPageId, catalogIndex);
            // TODO update root in catalog

            bf.unpinPage(newRoot.getPid(), catalogIndex);
        } else {
            insertIntoParent(node, promotedKey, newInternal);
        }

        // mark dirty and unpin
        bf.unpinPage(newInternal.getPid(), catalogIndex);
    }

    private static byte[] removeTrailingBytes(byte[] input) {
        int endIndex = input.length;
        for (int i = input.length - 1; i >= 0; i--) {
            if (input[i] != PADDING_BYTE) { // Only remove custom padding byte
                endIndex = i + 1;
                break;
            }
        }
        return Arrays.copyOf(input, endIndex);
    }

    @Override
    public Iterator<Rid> search(String key) {
        IndexPage leaf = findLeaf(key);
        bf.unpinPage(leaf.getPid(), catalogIndex);

        int index = binarySearch(leaf.keys, key);
        if (index >= 0) {
            List<Rid> result = new ArrayList<>();
            while (index < leaf.keys.size()
                    && key.compareTo(new String(removeTrailingBytes(leaf.keys.get(index)))) == 0) {
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
            int cmp = new String(removeTrailingBytes(keys.get(mid))).compareTo(target);
            if (cmp == 0)
                return mid;
            if (cmp < 0)
                low = mid + 1;
            else
                high = mid - 1;
        }
        return -1;
    }

    @Override
    public Iterator<Rid> rangeSearch(String startKey, String endKey) {
        IndexPage leaf = findLeaf(startKey);
        List<Rid> result = new ArrayList<>();

        while (leaf != null) {
            for (int i = 0; i < leaf.keys.size(); i++) {
                if (new String(removeTrailingBytes(leaf.keys.get(i))).compareTo(startKey) >= 0
                        && new String(removeTrailingBytes(leaf.keys.get(i))).compareTo(endKey) <= 0) {
                    result.add(new Rid(bytesToInt(leaf.pageIds.get(i)), bytesToInt(leaf.slotIds.get(i))));
                } else if (new String(removeTrailingBytes(leaf.keys.get(i))).compareTo(endKey) > 0) {
                    // unpin the current leaf and return result
                    bf.unpinPage(leaf.getPid(), catalogIndex);
                    return result.iterator();
                }
            }
            bf.unpinPage(leaf.getPid(), catalogIndex);
            leaf = (IndexPage) bf.getPage(bytesToInt(leaf.nextLeaf), catalogIndex);
        }

        return result.iterator();
    }

    public static byte[] intToBytes(int value, int capacity) {
        // return ByteBuffer.allocate(capacity).putInt(value).array();
        byte[] bytes = new byte[capacity];
        for (int i = 0; i < capacity; i++) {
            bytes[capacity - 1 - i] = (byte) (value >>> (8 * i)); // Extract the required byte
        }
        return bytes;
    }

    // Convert a 4-byte array back to an integer
    public static int bytesToInt(byte[] bytes) {
        // return ByteBuffer.wrap(bytes).getInt();
        ByteBuffer buffer = ByteBuffer.allocate(4); // Ensure 4 bytes
        buffer.put(new byte[4 - bytes.length]); // Pad with leading zeros if needed
        buffer.put(bytes); // Copy the actual bytes
        buffer.rewind(); // Reset position before reading
        return buffer.getInt();
    }

    private static byte[] truncateOrPadByteArray(byte[] value, int maxLength) {
        if (value.length > maxLength) {
            return Arrays.copyOf(value, maxLength); // Truncate safely at byte level
        } else {
            byte[] padded = new byte[maxLength];
            System.arraycopy(value, 0, padded, 0, value.length); // Copy original bytes
            Arrays.fill(padded, value.length, maxLength, PADDING_BYTE); // Fill remaining space with 0x7F
            return padded;
        }
    }

    private IndexPage loadRootPage(int rootPageId) {
        return (IndexPage) bf.getPage(rootPageId, catalogIndex);
    }
}
