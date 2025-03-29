package com.database.finalproject.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.database.finalproject.constants.PageConstants.*;

public class IndexPage implements Page{
    private byte[] pageArray = new byte[PAGE_SIZE];
    private final int pageId;
    boolean isLeaf;
    public List<byte[]> keys;
    public List<byte[]> pageIds;
    public List<byte[]> slotIds;
    public byte[] nextLeaf;
    public byte[] prevLeaf;

    int order;
    int index;
    // Used only for leaf nodes

    public IndexPage(int pageId, int index) {
        this.index = index;
        this.pageId = pageId;
        keys = new ArrayList<>();
        pageIds = new ArrayList<>();
        slotIds = new ArrayList<>();
        nextLeaf = new byte[PAGE_ID_SIZE];
        prevLeaf = new byte[PAGE_ID_SIZE];

    }

    public void setIsLeaf(boolean leaf) {
        isLeaf = leaf;
        order = isLeaf ? (index == MOVIE_ID_INDEX_PAGE_INDEX ? MOVIE_ID_LEAF_NODE_ORDER : MOVIE_TITLE_LEAF_NODE_ORDER):
                (index == MOVIE_ID_INDEX_PAGE_INDEX ? MOVIE_ID_NON_LEAF_NODE_ORDER : MOVIE_TITLE_NON_LEAF_NODE_ORDER);
    }

    public boolean getIsLeaf(){
        return isLeaf;
    }

    public int getOrder(){
        return order;
    }

    @Override
    public int getPid() {
        return pageId;
    }

    @Override
    public byte[] getByteArray() {
        Arrays.fill(pageArray, (byte) 0);
        pageArray[0] = (byte) (isLeaf ? 1 : 0);
        pageArray[1] = (byte) keys.size(); // Store number of keys
        int offset = 2;
        if (isLeaf) {
            System.arraycopy(nextLeaf, 0, pageArray, offset, PAGE_ID_SIZE);
            offset += PAGE_ID_SIZE;
            System.arraycopy(prevLeaf, 0, pageArray, offset, PAGE_ID_SIZE);
            offset += PAGE_ID_SIZE;
        }
        for (byte[] key : keys) {
            System.arraycopy(key, 0, pageArray, offset, index == MOVIE_ID_INDEX_PAGE_INDEX ? MOVIE_ID_SIZE : MOVIE_TITLE_SIZE);
            offset += index == MOVIE_ID_INDEX_PAGE_INDEX ? MOVIE_ID_SIZE : MOVIE_TITLE_SIZE;
        }

        // Store all page IDs sequentially
        for (byte[] pageId : pageIds) {
            System.arraycopy(pageId, 0, pageArray, offset, PAGE_ID_SIZE);
            offset += PAGE_ID_SIZE;
        }

        // Store all slot IDs sequentially (only for leaf nodes)
        if (isLeaf) {
            for (byte[] slotId : slotIds) {
                System.arraycopy(slotId, 0, pageArray, offset, SLOT_ID_SIZE);
                offset += SLOT_ID_SIZE;
            }
        }

        return pageArray;
    }

    public void setByteArray(byte[] arr){
        System.arraycopy(arr, 0, pageArray, 0, PAGE_SIZE);

        // Read leaf flag and numKeys
        isLeaf = pageArray[0] == 1;
        int numKeys = pageArray[1];
        int offset = 2;

        // Read nextLeaf and prevLeaf (only for leaf nodes)
        if (isLeaf) {
            System.arraycopy(pageArray, offset, nextLeaf, 0, PAGE_ID_SIZE);
            offset += PAGE_ID_SIZE;
            System.arraycopy(pageArray, offset, prevLeaf, 0, PAGE_ID_SIZE);
            offset += PAGE_ID_SIZE;
        }

        // Read all keys
        keys.clear();
        for (int i = 0; i < numKeys; i++) {
            byte[] key = new byte[MOVIE_TITLE_SIZE];
            System.arraycopy(pageArray, offset, key, 0, index == MOVIE_ID_INDEX_PAGE_INDEX ? MOVIE_ID_SIZE : MOVIE_TITLE_SIZE);
            keys.add(key);
            offset += index == MOVIE_ID_INDEX_PAGE_INDEX ? MOVIE_ID_SIZE : MOVIE_TITLE_SIZE;
        }

        // Read all page IDs
        pageIds.clear();
        for (int i = 0; i < numKeys; i++) {
            byte[] pageId = new byte[PAGE_ID_SIZE];
            System.arraycopy(pageArray, offset, pageId, 0, PAGE_ID_SIZE);
            pageIds.add(pageId);
            offset += PAGE_ID_SIZE;
        }

        // Read all slot IDs (only for leaf nodes)
        slotIds.clear();
        if (isLeaf) {
            for (int i = 0; i < numKeys; i++) {
                byte[] slotId = new byte[SLOT_ID_SIZE];
                System.arraycopy(pageArray, offset, slotId, 0, SLOT_ID_SIZE);
                slotIds.add(slotId);
                offset += SLOT_ID_SIZE;
            }
        }
    }

}
