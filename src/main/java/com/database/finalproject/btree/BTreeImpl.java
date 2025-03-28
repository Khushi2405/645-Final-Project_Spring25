package com.database.finalproject.btree;

import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.model.MovieTitleIndexPage;
import com.database.finalproject.model.Rid;

import java.nio.ByteBuffer;
import java.util.*;

import static com.database.finalproject.constants.PageConstants.*;

public class BTreeImpl implements BTree<String, Rid> {

    BufferManager bf;
    int rootPageId;
    MovieTitleIndexPage rootPage;
    public BTreeImpl(BufferManager bf) {
        this.bf = bf;
        rootPageId = Integer.parseInt(bf.getRootPageId(2));
        rootPage = (MovieTitleIndexPage) bf.getPage(rootPageId, 2);
        if(rootPage == null) rootPage = (MovieTitleIndexPage) bf.createPage(2);
    }

    @Override
    public void insert(String key, Rid rid) {
        MovieTitleIndexPage leaf = findLeaf(key);
        insertIntoLeaf(leaf, key, rid.getPageId(), rid.getSlotId());
        if (leaf.keys.size() >= leaf.getOrder()) {
            splitLeafNode(leaf);
        }
    }

    private MovieTitleIndexPage findLeaf(String movieTitle) {
        MovieTitleIndexPage nodePage = rootPage;
        while (!nodePage.getIsLeaf()) {
            int i = 0;
            while (i < nodePage.keys.size() && movieTitle.compareTo(Arrays.toString(removeTrailingBytes(nodePage.keys.get(i)))) > 0) {
                i++;
            }
            nodePage = (MovieTitleIndexPage) bf.getPage(bytesToInt(nodePage.pageIds.get(i)), 2);
        }
        return nodePage;
    }

    private void insertIntoLeaf(MovieTitleIndexPage leafPage, String movieTitle, int pageId, int slotId) {
        int i = 0;
        while (i < leafPage.keys.size() &&  movieTitle.compareTo(Arrays.toString(removeTrailingBytes(leafPage.keys.get(i)))) > 0) {
            i++;
        }
        leafPage.keys.add(i, truncateOrPadByteArray(movieTitle.getBytes(), MOVIE_TITLE_SIZE));
        leafPage.pageIds.add(i, intToBytes(pageId, PAGE_ID_SIZE));
        leafPage.slotIds.add(i, intToBytes(slotId, SLOT_ID_SIZE));
    }

    private void splitLeafNode(MovieTitleIndexPage leafPage) {
        int mid = leafPage.keys.size() / 2;
        MovieTitleIndexPage newLeafPage = (MovieTitleIndexPage) bf.createPage(2);
        newLeafPage.setIsLeaf(true);
        newLeafPage.keys.addAll(leafPage.keys.subList(mid, leafPage.keys.size()));
        newLeafPage.pageIds.addAll(leafPage.pageIds.subList(mid, leafPage.pageIds.size()));
        newLeafPage.slotIds.addAll(leafPage.slotIds.subList(mid, leafPage.slotIds.size()));

        leafPage.keys.subList(mid, leafPage.keys.size()).clear();
        leafPage.pageIds.subList(mid, leafPage.pageIds.size()).clear();
        leafPage.slotIds.subList(mid, leafPage.slotIds.size()).clear();

        MovieTitleIndexPage nextLeafPage = (MovieTitleIndexPage) bf.getPage(bytesToInt(leafPage.nextLeaf), 2);
        newLeafPage.nextLeaf = intToBytes(nextLeafPage.getPid(), PAGE_ID_SIZE);
        nextLeafPage.prevLeaf = intToBytes(newLeafPage.getPid(), PAGE_ID_SIZE);
        leafPage.nextLeaf = intToBytes(newLeafPage.getPid(), PAGE_ID_SIZE);
        newLeafPage.prevLeaf = intToBytes(leafPage.getPid(), PAGE_ID_SIZE);

        insertIntoParent(leafPage, newLeafPage.keys.get(0), newLeafPage);
    }

    private void insertIntoParent(MovieTitleIndexPage leftPage, byte[] key, MovieTitleIndexPage rightPage) {
        if (leftPage == rootPage) {
            MovieTitleIndexPage newRootPage = (MovieTitleIndexPage) bf.createPage(2);
            newRootPage.setIsLeaf(false);
            newRootPage.keys.add(key);
            newRootPage.pageIds.add(intToBytes(leftPage.getPid(), PAGE_ID_SIZE));

            newRootPage.pageIds.add(intToBytes(rightPage.getPid(), PAGE_ID_SIZE));
            rootPage = newRootPage;
            //TODO update new root in catalog
            return;
        }

        MovieTitleIndexPage parent = findParent(rootPage, leftPage);
        int insertIdx = 0;
        while (insertIdx < parent.pageIds.size() && bytesToInt(parent.pageIds.get(insertIdx)) != leftPage.getPid()) {
            insertIdx++;
        }

        parent.keys.add(insertIdx, key);
        parent.pageIds.add(insertIdx + 1, intToBytes(rightPage.getPid(), PAGE_ID_SIZE));

        if (parent.keys.size() >= MOVIE_TITLE_NON_LEAF_NODE_ORDER) {
            splitInternalNode(parent);
        }
    }

    private MovieTitleIndexPage findParent(MovieTitleIndexPage node, MovieTitleIndexPage child) {
        if (node.getIsLeaf()) return null;
        for (byte[] n : node.pageIds) {
            int subChildPageId = bytesToInt(n);
            if (subChildPageId == child.getPid()) return node;
            MovieTitleIndexPage subChild = (MovieTitleIndexPage) bf.getPage(subChildPageId,2);
            MovieTitleIndexPage parent = findParent(subChild, child);
            if (parent != null) return parent;
        }
        return null;
    }

    private void splitInternalNode(MovieTitleIndexPage node) {
        int mid = node.keys.size() / 2;
        MovieTitleIndexPage newInternal = (MovieTitleIndexPage) bf.createPage(2);
        newInternal.setIsLeaf(false);

        newInternal.keys.addAll(node.keys.subList(mid + 1, node.keys.size()));
        newInternal.pageIds.addAll(node.pageIds.subList(mid + 1, node.pageIds.size()));

        byte[] promotedKey = node.keys.get(mid);
        node.keys.subList(mid, node.keys.size()).clear();
        node.pageIds.subList(mid + 1, node.pageIds.size()).clear();

        if (node == rootPage) {
            MovieTitleIndexPage newRoot = (MovieTitleIndexPage) bf.createPage(2);
            newRoot.keys.add(promotedKey);
            newRoot.pageIds.add(intToBytes(node.getPid(), PAGE_ID_SIZE));
            newRoot.pageIds.add(intToBytes(newInternal.getPid(), PAGE_ID_SIZE));
            rootPage = newRoot;
            //TODO update root in catalog
        } else {
            insertIntoParent(node, promotedKey, newInternal);
        }
    }


    private static byte[] removeTrailingBytes(byte[] input) {
        int endIndex = input.length;
        for (int i = input.length - 1; i >= 0; i--) {
            if (input[i] != PADDING_BYTE) {  // Only remove custom padding byte
                endIndex = i + 1;
                break;
            }
        }
        return Arrays.copyOf(input,endIndex);
    }


    @Override
    public Iterator<Rid> search(String key) {
        MovieTitleIndexPage leaf = findLeaf(key);
        int index = binarySearch(leaf.keys, key);

        if (index >= 0) {
            List<Rid> result = new ArrayList<>();
            while (index < leaf.keys.size() && key.compareTo(Arrays.toString(removeTrailingBytes(leaf.keys.get(index)))) == 0) {
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
            int cmp = Arrays.toString(removeTrailingBytes(keys.get(mid))).compareTo(target);
            if (cmp == 0) return mid;
            if (cmp < 0) low = mid + 1;
            else high = mid - 1;
        }
        return -1;
    }

    @Override
    public Iterator<Rid> rangeSearch(String startKey, String endKey) {
        MovieTitleIndexPage leaf = findLeaf(startKey);
        List<Rid> result = new ArrayList<>();

        while (leaf != null) {
            for (int i = 0; i < leaf.keys.size(); i++) {
                if (Arrays.toString(removeTrailingBytes(leaf.keys.get(i))).compareTo(startKey) >= 0 && Arrays.toString(removeTrailingBytes(leaf.keys.get(i))).compareTo(endKey) <= 0) {
                    result.add(new Rid(bytesToInt(leaf.pageIds.get(i)), bytesToInt(leaf.slotIds.get(i))));
                } else if (Arrays.toString(removeTrailingBytes(leaf.keys.get(i))).compareTo(endKey) > 0) {
                    return result.iterator();
                }
            }
            leaf = (MovieTitleIndexPage) bf.getPage(bytesToInt(leaf.nextLeaf),2);
        }
        return result.iterator();
    }

    public static byte[] intToBytes(int value, int capacity) {
        return ByteBuffer.allocate(capacity).putInt(value).array();
    }

    // Convert a 4-byte array back to an integer
    public static int bytesToInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
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
}
