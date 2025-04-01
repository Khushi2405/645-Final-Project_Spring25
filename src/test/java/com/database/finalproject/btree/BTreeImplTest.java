package com.database.finalproject.btree;

import com.database.finalproject.buffermanager.*;
import com.database.finalproject.model.IndexPage;
import com.database.finalproject.model.Rid;

import java.util.ArrayList;
import java.util.Iterator;

import static com.database.finalproject.constants.PageConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import java.util.List;

class BTreeImplTest {
    
    private BTreeImpl bTree;
    private Logger mockLogger;

    //@Mock
    private BufferManager bufferManager;

    @BeforeEach
    void setUp() {
        bufferManager = Mockito.mock(BufferManagerImpl.class);
        
        // 1. Mock initial state - no root exists
        when(bufferManager.getRootPageId(MOVIE_ID_INDEX_PAGE_INDEX)).thenReturn("-1");
        
        // 2. Mock root page creation
        IndexPage mockRootPage = new IndexPage(0, MOVIE_ID_INDEX_PAGE_INDEX);
        mockRootPage.setIsLeaf(false); // Root starts as non-leaf
        
        // 3. Mock leaf page that root points to
        IndexPage mockLeafPage = new IndexPage(1, MOVIE_ID_INDEX_PAGE_INDEX);
        mockLeafPage.setIsLeaf(true);
        
        // Set up root to point to leaf
        mockRootPage.pageIds.add(intToBytes(1, PAGE_ID_SIZE));
        
        // Mock page creation and retrieval
        when(bufferManager.createPage(MOVIE_ID_INDEX_PAGE_INDEX))
            .thenReturn(mockRootPage)  // First call creates root
            .thenReturn(mockLeafPage); // Second call creates leaf
        
        when(bufferManager.getPage(0, MOVIE_ID_INDEX_PAGE_INDEX)).thenReturn(mockRootPage);
        when(bufferManager.getPage(1, MOVIE_ID_INDEX_PAGE_INDEX)).thenReturn(mockLeafPage);
        
        // Create BTree which will initialize the structure
        bTree = new BTreeImpl(bufferManager, MOVIE_ID_INDEX_PAGE_INDEX);
    }

    @Test
    void testInitializationCreatesRootPage() {
        verify(bufferManager, times(1)).createPage(MOVIE_ID_INDEX_PAGE_INDEX);
        verify(bufferManager, times(1)).setRootPageId(anyInt(), eq(MOVIE_ID_INDEX_PAGE_INDEX));
    }
    @Test
    void testInsertSingleKey() {
        Rid rid = new Rid(1, 0);
        bTree.insert("tt0000001", rid);
        
        verify(bufferManager, atLeastOnce()).markDirty(anyInt(), eq(MOVIE_ID_INDEX_PAGE_INDEX));
        verify(bufferManager, atLeastOnce()).unpinPage(anyInt(), eq(MOVIE_ID_INDEX_PAGE_INDEX));
    }

    @Test
    void testInsertMultipleKeys() {
        Rid rid1 = new Rid(1, 0);
        Rid rid2 = new Rid(2, 0);
        Rid rid3 = new Rid(3, 0);

        bTree.insert("tt0000001", rid1);
        bTree.insert("tt0000002", rid2);
        bTree.insert("tt0000003", rid3);
    
        verify(bufferManager, atLeast(3)).markDirty(anyInt(), eq(MOVIE_ID_INDEX_PAGE_INDEX));
        verify(bufferManager, atLeast(3)).unpinPage(anyInt(), eq(MOVIE_ID_INDEX_PAGE_INDEX));
    }

    @Test
    void testSearchExistingKey() {
        Rid rid = new Rid(1, 0);
        bTree.insert("tt0000001", rid);
        
        // Search for the key "tt0000001"
        Iterator<Rid> resultIterator = bTree.search("tt0000001");
        
        // Ensure that the iterator is not null
        assertNotNull(resultIterator);
        
        // Check if the iterator has results
        assertTrue(resultIterator.hasNext());
        
        // Retrieve the first result
        Rid firstResult = resultIterator.next();
        
        // Perform field-by-field comparison (instead of using equals)
        assertNotNull(firstResult);
        assertEquals(rid.getPageId(), firstResult.getPageId()); // Compare pageId
        assertEquals(rid.getSlotId(), firstResult.getSlotId()); // Compare slotId
        
        // Check that there are no more results (since only one result is expected)
        assertFalse(resultIterator.hasNext());
    }

    @Test
    void testSearchNonExistingKey() {
        Iterator<Rid> resultIterator = bTree.search("tt9999999");
        assertNotNull(resultIterator);
        assertFalse(resultIterator.hasNext());
    }

    @Test
    void testSplitOnInsert() {
        for (int i = 1; i <= 10; i++) {
            bTree.insert("tt000000" + i, new Rid(i, 0));
        }
    
        verify(bufferManager, atLeastOnce()).createPage(MOVIE_ID_INDEX_PAGE_INDEX);
        verify(bufferManager, atLeastOnce()).markDirty(anyInt(), eq(MOVIE_ID_INDEX_PAGE_INDEX));
    }
    @Test
    public void testRangeSearch() {
        bTree.insert("tt0000001", new Rid(1, 10));
        bTree.insert("tt0000002", new Rid(2, 20));
        bTree.insert("tt0000003", new Rid(3, 30));
        bTree.insert("tt0000004", new Rid(4, 40));
        bTree.insert("tt0000005", new Rid(5, 50));

        Iterator<Rid> result = bTree.rangeSearch("tt0000002", "tt0000004");
        assertTrue(result.hasNext());

        assertEquals((new Rid(2, 20)).getPageId(), result.next().getPageId());
        assertEquals((new Rid(3, 30)).getPageId(), result.next().getPageId());
        assertEquals((new Rid(4, 40)).getPageId(), result.next().getPageId());

        // assertEquals(new Rid(2, 20)), result.next());
        // assertEquals(new Rid(3, 30), result.next());
        // assertEquals(new Rid(4, 40), result.next());
        assertFalse(result.hasNext());
    }
}

