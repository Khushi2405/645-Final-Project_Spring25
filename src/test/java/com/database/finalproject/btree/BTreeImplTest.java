package com.database.finalproject.btree;

import com.database.finalproject.buffermanager.*;
import com.database.finalproject.model.IndexPage;
import com.database.finalproject.model.Rid;

import java.util.ArrayList;

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

}

