package com.database.finalproject.buffermanager;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.database.finalproject.model.Page;
import com.database.finalproject.model.PageNotFoundException;
import com.database.finalproject.model.Row;

class BufferManagerImplTest {
    private BufferManager bufferManager;

    @BeforeEach
    void setUp() {
        bufferManager = new BufferManagerImpl(5);
    }

    // page
    @Test //verifies that pages are being created 
    void testCreatePage() {
        Page page = bufferManager.createPage();
        assertNotNull(page, "Page should be created successfully");
        assertTrue(page.getPid() >= 0, "Page ID should be valid");
    }

    @Test //tests that pages are being fetched
    void testFetchPage() {
        Page createdPage = bufferManager.createPage();
        int pageId = createdPage.getPid();

        Page fetchedPage = bufferManager.getPage(pageId);
        assertNotNull(fetchedPage, "Fetched page should not be null");
        assertEquals(pageId, fetchedPage.getPid(), "Fetched page ID should match");
    }

    @Test //tests that a new page is not created if buffer is full and all pages are pinned
    void testBufferFullForCreatePage() {
        BufferManagerImpl bufferManagerSpy = spy(new BufferManagerImpl(5));
        for (int i = 1; i <= 5; i++) {
            Page createdPage = bufferManagerSpy.createPage();
        }
        Page createdPageExtra = bufferManagerSpy.createPage();

        assertNull(createdPageExtra, "Page should not be create, response should be NULL");
    }

    @Test //tests that buffer cannot get a page that exists but is not in buffer, if buffer is full and all pages are pinned
    void testBufferFullForGetPage() {
        BufferManagerImpl bufferManagerSpy = spy(new BufferManagerImpl(5));
        for (int i = 1; i <= 4; i++) {
            Page createdPage = bufferManagerSpy.createPage();
        }
        Page page5 = bufferManagerSpy.createPage();
        int pageId5 = page5.getPid();
        bufferManagerSpy.unpinPage(pageId5);

        Page newPage = bufferManagerSpy.createPage();

        Page getPage5 = bufferManagerSpy.getPage(pageId5);

        assertNull(getPage5, "Buffer full: Cannot get page, response should be NULL");
    }

    @Test // tests
    void testWritePageCall() {
        BufferManagerImpl bufferManagerSpy = spy(new BufferManagerImpl(5));
        for (int i = 1; i <= 4; i++) {
            Page createdPage = bufferManagerSpy.createPage();
        }
        Page page5 = bufferManagerSpy.createPage();
        int pageId5 = page5.getPid();
        bufferManagerSpy.unpinPage(pageId5);

        Page newPage = bufferManagerSpy.createPage();
        verify(bufferManagerSpy).writeToBinaryFile(page5);
    }

    @Test //tests that createPage() is calling markDirty()
    void testMarkDirtyOnCreatePage() {
        BufferManagerImpl bufferManagerSpy = spy(new BufferManagerImpl(5));

        Page newPage = bufferManagerSpy.createPage();
        int pageId = newPage.getPid();

        // Verify if markDirty(pageId) was called inside createPage()
        verify(bufferManagerSpy).markDirty(pageId);
    }

    @Test //tests that LRU Eviction work properly
    void testLRUEviction() {
        // Fill the buffer pool
        Page page1 = bufferManager.createPage();
        Page page2 = bufferManager.createPage();
        Page page3 = bufferManager.createPage();
        Page page4 = bufferManager.createPage();
        Page page5 = bufferManager.createPage();

        // Access page1 to make it recently used
        bufferManager.getPage(page1.getPid());

        // Create a new page (should evict page2)
        Page newPage = bufferManager.createPage();

        assertNotNull(bufferManager.getPage(page1.getPid()), "Page1 should still be in the buffer");
    }

    // rows
    @Test // tests that row insertions are working correctly 
    void testInsertRow() {
        Page page = bufferManager.createPage();
        Row row = new Row("tt1111111".getBytes(StandardCharsets.UTF_8),
                "Test Movie Insert".getBytes(StandardCharsets.UTF_8));

        int rowId = page.insertRow(row);
        assertEquals(0, rowId, "Row should be inserted at index 0");

        Row fetchedRow = page.getRow(rowId);
        assertNotNull(fetchedRow, "Inserted row should be retrievable");
    }

    @Test //tests that dirty pages are written to file 
    void testwriteToBinaryFileInsertRow() {
        BufferManagerImpl bufferManagerSpy = spy(new BufferManagerImpl(5));
        // make 4 pages and pin
        for (int i = 1; i <= 4; i++) {
            Page createdPage = bufferManagerSpy.createPage();
        }
        // create 5th page and unpin
        Page page5 = bufferManagerSpy.createPage();
        int pageId5 = page5.getPid();
        bufferManagerSpy.unpinPage(pageId5);

        // create 6th page, page 5 will be evicted and unpin page 6
        Page page6 = bufferManagerSpy.createPage();
        int pageId6 = page6.getPid();
        bufferManagerSpy.unpinPage(pageId6);

        // get page5 [page6 gets evicted], insert rows, mark dirty, unpin
        Page getPage5 = bufferManagerSpy.getPage(pageId5);
        Row row = new Row("tt1111111".getBytes(StandardCharsets.UTF_8),
                "Test Movie Insert".getBytes(StandardCharsets.UTF_8));
        int rowId = getPage5.insertRow(row) - 1;
        bufferManagerSpy.markDirty(pageId5);
        bufferManagerSpy.unpinPage(pageId5);

        // get page6 [5 gets evicted, should call writeToBinaryFile]
        Page getPage6 = bufferManagerSpy.getPage(pageId6);
        verify(bufferManagerSpy).writeToBinaryFile(getPage5);
    }

    // markDirty exception
    @Test // throws page not found exception when invalid page is marked dirty
    void testMarkDirtyThrowsExceptionWhenPageNotFound() {
        int invalidPageId = 999;

        Exception exception = assertThrows(PageNotFoundException.class, () -> {
            bufferManager.markDirty(invalidPageId);
        });

        assertEquals("No page with this ID - " + invalidPageId, exception.getMessage());
    }

    // unpin Exception
    @Test // throws page not found exception when you try to unpin a page that does not exist
    void testUnpinPageThrowsExceptionWhenPageNotFound() {
        int invalidPageId = 999;

        Exception exception = assertThrows(PageNotFoundException.class, () -> {
            bufferManager.unpinPage(invalidPageId);
        });

        assertEquals("No page with this ID - " + invalidPageId, exception.getMessage());
    }

}