package com.database.finalproject.buffermanager;

import com.database.finalproject.model.DLLNode;
import com.database.finalproject.model.Page;
import com.database.finalproject.model.PageImpl;
import com.database.finalproject.model.PageNotFoundException;
import com.database.finalproject.model.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;

import static com.database.finalproject.constants.PageConstants.*;

@Component
public class BufferManagerImpl extends BufferManager {

    DLLNode headBufferPool, tailBufferPool;
    Map<Integer, DLLNode> pageHash;
    int pageCount;
    public static Logger logger = LoggerFactory.getLogger(BufferManagerImpl.class);

    public BufferManagerImpl(@Value("${buffer.size:10}") int bufferSize) {
        super(bufferSize);
        this.pageHash = new HashMap<>();
        tailBufferPool = null;
        headBufferPool = null;
        pageCount = 0;
    }

    @Override
    public Page getPage(int pageId) {
        // Logic to fetch a page from buffer

        // if page id already in buffer then move it to front and increment the pin
        // counter
        if (pageHash.containsKey(pageId)) {
            DLLNode currNode = pageHash.get(pageId);
            bringPageFront(currNode);
            currNode.pinCount++;
            return currNode.page;
        } else {
            if (pageHash.size() > bufferSize - 1) {
                // implement LRU
                if (!removeLRUNode()) {
                    // buffer manager is full throw error
                    logger.error("Buffer manager is full cannot fetch new pages");
                    return null;
                }
            }
            try {
                Page page = readPage(pageId);

                // if page is null then page with the page id not found return null
                if (page == null) {
                    logger.error("Page not found: {}", pageId);
                    return null;
                }
                DLLNode currNode = new DLLNode(page);
                addNewPage(pageId, currNode);
                return page;
            } catch (IOException e) {
                logger.error("Cannot read file");
                return null;
            }

        }
    }

    @Override
    public Page createPage() {
        // Logic to create a new page

        if (pageHash.size() > bufferSize - 1) {
            if (!removeLRUNode()) {
                // buffer manager is full throw error
                logger.error("Buffer manager is full cannot create new pages");
                return null;
            }
        }
        Page page = new PageImpl(pageCount++);
        DLLNode currNode = new DLLNode(page);
        addNewPage(page.getPid(), currNode);
        return page;
    }

    @Override
    public void markDirty(int pageId) {
        // Mark page as dirty
        if (pageHash.containsKey(pageId)) {
            pageHash.get(pageId).isDirty = true;
            return;
        }
        logger.error("Page not found: {}", pageId);
    }

    @Override
    public void unpinPage(int pageId) {
        // Unpin page
        if (pageHash.containsKey(pageId)) {
            pageHash.get(pageId).pinCount--;
            return;
        }
        logger.error("Page not found: {}", pageId);
    }

    @Override
    public void writeToBinaryFile(Page page) {
        try (RandomAccessFile raf = new RandomAccessFile(INPUT_FILE, "rwd")) {
            long offset = (long) (page.getPid()) * PAGE_SIZE;
            raf.seek(offset);

            byte[] pageData = page.getRows();

            raf.write(pageData);
            raf.getFD().sync();

            System.out.println("Updated page " + page.getPid() + " successfully!");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void bringPageFront(DLLNode currNode) {
        // if node is already at front don't do anything
        if (currNode == headBufferPool)
            return;

        // if node is at tail just point the prev node to null
        else if (currNode == tailBufferPool) {
            DLLNode prev = currNode.prev;
            prev.next = null;
            tailBufferPool = prev;
        }

        // if node in middle of list point to prev and next nodes to each other
        else {
            DLLNode prev = currNode.prev;
            DLLNode next = currNode.next;
            prev.next = next;
            next.prev = prev;
        }

        // now add the curr node to starting of list and change the head pointer
        currNode.prev = null;
        currNode.next = headBufferPool;
        headBufferPool.prev = currNode;
        headBufferPool = currNode;
    }

    private Page readPage(int pageId) throws IOException {
        Page page = new PageImpl(pageId);
        try (RandomAccessFile raf = new RandomAccessFile(INPUT_FILE, "r")) {
            long offset = (long) (pageId) * PAGE_SIZE;
            if (raf.length() <= offset) {
                logger.error("Page not found: {}", pageId);
                return null;
            }
            raf.seek(offset);
            byte[] pageData = new byte[PAGE_SIZE]; // Buffer to hold 4096 bytes (one page)
            raf.readFully(pageData);
            int nextRow = binaryToDecimal(pageData[PAGE_SIZE - 1]);
            for (int i = 0; i < nextRow; i++) {
                int offsetInPage = i * 39;
                byte[] movieId = Arrays.copyOfRange(pageData, offsetInPage, offsetInPage + 9);
                byte[] movieTitle = Arrays.copyOfRange(pageData, offsetInPage + 9, offsetInPage + 39);
                Row row = new Row(movieId, movieTitle);
                page.insertRow(row);
            }
        }
        return page;
    }

    private boolean removeLRUNode() {
        DLLNode unpinnedNode = tailBufferPool;
        while (unpinnedNode != null && unpinnedNode.pinCount != 0) {
            unpinnedNode = unpinnedNode.prev;
        }
        if (unpinnedNode == null) {
            // no such page with pincount 0
            // throw error
            return false;
        } else {
            if (unpinnedNode.isDirty) {
                writeToBinaryFile(unpinnedNode.page);
            }
            pageHash.remove(unpinnedNode.page.getPid());
            DLLNode prev = unpinnedNode.prev;
            DLLNode next = unpinnedNode.next;
            if (unpinnedNode == tailBufferPool) {
                prev.next = null;
                tailBufferPool = prev;
            } else if (unpinnedNode == headBufferPool) {
                headBufferPool = next;
            } else {
                prev.next = next;
                next.prev = prev;
            }
        }
        return true;
    }

    private void addNewPage(int pageId, DLLNode currNode) {
        pageHash.put(pageId, currNode);
        if (headBufferPool == tailBufferPool && tailBufferPool == null) {
            headBufferPool = currNode;
            tailBufferPool = currNode;
        } else {
            currNode.prev = null;
            currNode.next = headBufferPool;
            headBufferPool.prev = currNode;
            headBufferPool = currNode;

        }
        currNode.pinCount++;
        markDirty(pageId);
    }

    private static int binaryToDecimal(byte b) {
        return Integer.parseInt(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'), 2);
    }

}
