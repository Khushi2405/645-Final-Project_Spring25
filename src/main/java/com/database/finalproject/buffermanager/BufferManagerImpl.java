package com.database.finalproject.buffermanager;

import com.database.finalproject.model.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


import static com.database.finalproject.constants.PageConstants.*;

@Component
public class BufferManagerImpl extends BufferManager {

    DLLNode headBufferPool, tailBufferPool;
    Map<Pair<Integer, Integer>, DLLNode> pageHash;
    DatabaseCatalog catalog;
    RandomAccessFile dataRaf;
    RandomAccessFile movieIdIndexRaf;
    RandomAccessFile movieTitleRaf;
    public static Logger logger = LoggerFactory.getLogger(BufferManagerImpl.class);

    public BufferManagerImpl(@Value("${buffer.size:10}") int bufferSize) {
        super(bufferSize);
        catalog = new DatabaseCatalog("src/main/resources/static/database_catalog.txt");
        this.pageHash = new HashMap<>();
        tailBufferPool = null;
        headBufferPool = null;


        try {
            dataRaf = new RandomAccessFile(catalog.getCatalog(DATA_PAGE_INDEX).get("filename"), "rwd");
        } catch (IOException e) {
            System.out.println("Error in RAF, file cannot be created");
            throw new RuntimeException(e);
        }
        try {
            movieIdIndexRaf = new RandomAccessFile(catalog.getCatalog(MOVIE_ID_INDEX_PAGE_INDEX).get("filename"), "rwd");
        } catch (IOException e) {
            System.out.println("Error in RAF, file cannot be created");
            throw new RuntimeException(e);
        }
        try {
            movieTitleRaf = new RandomAccessFile(catalog.getCatalog(MOVIE_TITLE_INDEX_INDEX).get("filename"), "rwd");
        } catch (IOException e) {
            System.out.println("Error in RAF, file cannot be created");
            throw new RuntimeException(e);
        }
    }

    @Override
    public Page getPage(int pageId, int ...index) {
        // Logic to fetch a page from buffer
        int catalogIndex = getCatalogIndex(index);
        Pair<Integer, Integer> pair = new Pair(pageId, catalogIndex);
        // if page id already in buffer then move it to front and increment the pin counter
        if (pageHash.containsKey(pair)) {
            DLLNode currNode = pageHash.get(pair);
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
                Page page = readPage(pageId, catalogIndex);

                // if page is null then page with the page id not found return null
                if (page == null) {
                    logger.error("Page not found: {}", pageId);
                    return null;
                }
                DLLNode currNode = new DLLNode(page, catalogIndex);
                addNewPage(pair, currNode);
                return page;
            } catch (IOException e) {
                logger.error("Cannot read file");
                return null;
            }

        }
    }

    @Override
    public Page createPage(int ...index) {
        // Logic to create a new page

        int catalogIndex = getCatalogIndex(index);

        if (pageHash.size() > bufferSize - 1) {
            if (!removeLRUNode()) {
                // buffer manager is full throw error
                logger.error("Buffer manager is full cannot create new pages");
                return null;
            }
        }
        int pageCount = Integer.parseInt(catalog.getCatalog(catalogIndex).get("totalPages"));
        Page page;
        if(catalogIndex == DATA_PAGE_INDEX) {
            page = new DataPage(pageCount++);
        }
        else if(catalogIndex == MOVIE_ID_INDEX_PAGE_INDEX){
            page = new IndexPage(pageCount++, MOVIE_ID_INDEX_PAGE_INDEX);
        }
        else{
            page = new IndexPage(pageCount++, MOVIE_TITLE_INDEX_INDEX);
        }
        catalog.setCatalog(catalogIndex, "totalPages", String.valueOf(pageCount));
        DLLNode currNode = new DLLNode(page, catalogIndex);
        addNewPage(new Pair<>(page.getPid(), catalogIndex), currNode);
        markDirty(page.getPid(), index);
        return page;
    }

    @Override
    public void markDirty(int pageId, int ...index) {
        // Mark page as dirty

        int catalogIndex = getCatalogIndex(index);

        Pair<Integer, Integer> pair = new Pair<>(pageId, catalogIndex);
        if (pageHash.containsKey(pair)) {
            pageHash.get(pair).isDirty = true;
            return;
        }
        logger.error("Page not found: {}", pageId);
    }

    @Override
    public void unpinPage(int pageId, int ...index) {
        // Unpin page
        int catalogIndex = getCatalogIndex(index);
        Pair<Integer, Integer> pair = new Pair<>(pageId, catalogIndex);
        if (pageHash.containsKey(pair)) {
            pageHash.get(pair).pinCount--;
            return;
        }
        logger.error("Page not found: {}", pageId);
    }

    @Override
    public void writeToBinaryFile(Page page, int ...index) {
        int catalogIndex = getCatalogIndex(index);
        RandomAccessFile raf;
        if(catalogIndex == DATA_PAGE_INDEX) raf = dataRaf;
        else if(catalogIndex == MOVIE_ID_INDEX_PAGE_INDEX) raf = movieIdIndexRaf;
        else raf = movieTitleRaf;
        try {
            long offset = (long) (page.getPid()) * PAGE_SIZE;
            raf.seek(offset);

            byte[] pageData = page.getByteArray();

            raf.write(pageData);
            raf.getFD().sync();

            // System.out.println("Updated page " + page.getPid() + " successfully!");
        } catch (IOException e) {
            // System.out.println("Reached the exception");
            throw new RuntimeException(e);
        }
    }

    @Override
    public void force() {
        while(headBufferPool != null){
            if(headBufferPool.isDirty) {
                writeToBinaryFile(headBufferPool.page, headBufferPool.index);
            }
            headBufferPool = headBufferPool.next;
        }
        pageHash.clear();
        headBufferPool = null;
        tailBufferPool = null;
    }

    @Override
    public String getRootPageId(int ...index){
        int catalogIndex = getCatalogIndex(index);
        return catalog.getCatalog(catalogIndex).get(DATABASE_CATALOGUE_KEY_ROOT_PAGE);
    }

    @Override
    public void setRootPageId(int rootPageId, int... index) {
        int catalogIndex = getCatalogIndex(index);
        catalog.setCatalog(catalogIndex, DATABASE_CATALOGUE_KEY_ROOT_PAGE, String.valueOf(rootPageId));
    }

    @Override
    public String getFilePath(int index) {
        int catalogIndex = getCatalogIndex(index);
        return catalog.getCatalog(catalogIndex).get(DATABASE_CATALOGUE_KEY_FILENAME);
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

    private Page readPage(int pageId, int... index) throws IOException {
        int catalogIndex = getCatalogIndex(index);
        // TODO: read mode
        RandomAccessFile raf;
        if (catalogIndex == DATA_PAGE_INDEX)
            raf = dataRaf;
        else if (catalogIndex == MOVIE_ID_INDEX_PAGE_INDEX)
            raf = movieIdIndexRaf;
        else
            raf = movieTitleRaf;

        Page page = null;
        try {
            long offset = (long) (pageId) * PAGE_SIZE;
            if (raf.length() <= offset) {
                logger.error("Page not found: {}", pageId);
                return null;
            }
            raf.seek(offset);
            byte[] pageData = new byte[PAGE_SIZE]; // Buffer to hold 4096 bytes (one page)
            raf.readFully(pageData);

            if (catalogIndex == DATA_PAGE_INDEX) {
                page = readDataPage(pageId, pageData);
            } else {
                page = readIndexPage(pageId, pageData, catalogIndex);
            }
            // System.out.println(page.getRow(0));
            // System.out.println(page.getRow(PAGE_ROW_LIMIT - 1));
        } catch (IOException e) {
            logger.error("Cannot read file");
            return null;
        }
        return page;
    }

    // read data page
    private Page readDataPage(int pageId, byte[] pageData) {
        Page page = new DataPage(pageId);
        int nextRow = binaryToDecimal(pageData[PAGE_SIZE - 1]);
        for (int i = 0; i < nextRow; i++) {
            int offsetInPage = i * 39;
            byte[] movieId = Arrays.copyOfRange(pageData, offsetInPage, offsetInPage + 9);
            byte[] movieTitle = Arrays.copyOfRange(pageData, offsetInPage + 9, offsetInPage + 39);
            Row row = new Row(movieId, movieTitle);
            ((DataPage) page).insertRow(row);
        }
        return page;
    }

    // read index page
    private Page readIndexPage(int pageId, byte[] pageData, int catalogIndex) {
        Page page;
        if (catalogIndex == MOVIE_ID_INDEX_PAGE_INDEX) {
            page = new IndexPage(pageId, MOVIE_ID_INDEX_PAGE_INDEX);
            ((IndexPage) page).setByteArray(pageData);
        } else {
            page = new IndexPage(pageId, MOVIE_TITLE_INDEX_INDEX);
            ((IndexPage) page).setByteArray(pageData);
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
                writeToBinaryFile(unpinnedNode.page, unpinnedNode.index);
            }
            pageHash.remove(new Pair<>(unpinnedNode.page.getPid(), unpinnedNode.index));
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

    private void addNewPage(Pair<Integer, Integer> pair, DLLNode currNode) {
        pageHash.put(pair, currNode);
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
    }

    private static int binaryToDecimal(byte b) {
        return Integer.parseInt(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'), 2);
    }

    private int getCatalogIndex(int ...index){
        int catalogIndex;
        if(index.length == 0) catalogIndex = DATA_PAGE_INDEX;
        else catalogIndex = index[0];
        return catalogIndex;
    }

}
