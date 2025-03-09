package com.database.finalproject.buffermanager;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

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
    //createPage tests
    @Test
    void testCreatePage() {
        // create a new page
        Page page = bufferManager.createPage();
        assertNotNull(page, "Page should be created successfully");
        assertTrue(page.getPid() >= 0, "Page ID should be valid");
    }

    @Test
    void testBufferFullForCreatePage() {
        // buffer full for createPage
        BufferManagerImpl bufferManagerSpy = spy(new BufferManagerImpl(5));
        for (int i = 1; i <= 5; i++) {
            Page createdPage = bufferManagerSpy.createPage();
        }
        Page createdPageExtra = bufferManagerSpy.createPage();

        assertNull(createdPageExtra, "Page should not be create, response should be NULL");
    }

    // getPage tests
    @Test
    void testFetchPage() {
        // page already in buffer pool
        Page createdPage = bufferManager.createPage();
        int pageId = createdPage.getPid();

        Page fetchedPage = bufferManager.getPage(pageId);
        assertNotNull(fetchedPage, "Fetched page should not be null");
        assertEquals(pageId, fetchedPage.getPid(), "Fetched page ID should match");
    }

    @Test
    void testFetchPageNotInBuffer() {
        // page not in buffer pool
        for(int i = 1; i <= 4; i++) {
            Page createdPage = bufferManager.createPage();
        }
        Page page5 = bufferManager.createPage();
        int pageId5 = page5.getPid();
        bufferManager.unpinPage(pageId5);

        // evict page 5
        Page page6 = bufferManager.createPage();
        int pageId6 = page6.getPid();
        bufferManager.unpinPage(pageId6);

        // get page 5, evict 6
        Page fetchedPage = bufferManager.getPage(pageId5);
        assertNotNull(fetchedPage, "Fetched page should not be null");
        assertEquals(pageId5, fetchedPage.getPid(), "Fetched page ID should match");
    }

    @Test
    void testBufferFullForGetPage() {
        // buffer full on getPage
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

    @Test
    void testWritePageCall() {
        //empty page writes
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

    @Test
    void testMarkDirtyOnCreatePage() {
        // page to be marked dirty on createPage
        BufferManagerImpl bufferManagerSpy = spy(new BufferManagerImpl(5));

        Page newPage = bufferManagerSpy.createPage();
        int pageId = newPage.getPid();

        // Verify if markDirty(pageId) was called inside createPage()
        verify(bufferManagerSpy).markDirty(pageId);
    }

    @Test
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
    @Test
    void testInsertRow() {
        Page page = bufferManager.createPage();
        Row row = new Row("tt1111111".getBytes(StandardCharsets.UTF_8),
                "Test Movie Insert".getBytes(StandardCharsets.UTF_8));

        int rowId = page.insertRow(row);
        assertEquals(0, rowId, "Row should be inserted at index 0");

        Row fetchedRow = page.getRow(rowId);
        assertNotNull(fetchedRow, "Inserted row should be retrievable");

        assertFalse(page.isFull(), "Page is not full.");
    }

    @Test
    void testPageFull() {
        Page page = bufferManager.createPage();
        for(int i = 1; i <= 105; i++) {
            Row row = new Row("tt1111111".getBytes(StandardCharsets.UTF_8),
                    "Test Movie Insert".getBytes(StandardCharsets.UTF_8));
            int rowId = page.insertRow(row);
        }

        assertTrue(page.isFull(), "Page is full.");

        Row row = new Row("tt1111111".getBytes(StandardCharsets.UTF_8),
                "Test Movie Insert".getBytes(StandardCharsets.UTF_8));
        int rowId = page.insertRow(row);
        assertEquals(-1, rowId, "Should return -1 on new insertion if page is full");
    }

    @Test
    void testWriteToBinaryFileInsertRow() {
        // write to binary file when page with rows inserted and marked dirty is evicted
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
    @Test
    void testMarkDirtyThrowsExceptionWhenPageNotFound() {
        int invalidPageId = 999;

        Exception exception = assertThrows(PageNotFoundException.class, () -> {
            bufferManager.markDirty(invalidPageId);
        });

        assertEquals("No page with this ID - " + invalidPageId, exception.getMessage());
    }

    @Test
    void testMarkDirtyDoesNotThrowError() {
        Page newPage = bufferManager.createPage();
        int pageId = newPage.getPid();
        assertDoesNotThrow(() -> bufferManager.markDirty(pageId),
                "markDirty should not throw an error when page exists in buffer");
    }

    // unpin Exception
    @Test
    void testUnpinPageThrowsExceptionWhenPageNotFound() {
        int invalidPageId = 999;

        Exception exception = assertThrows(PageNotFoundException.class, () -> {
            bufferManager.unpinPage(invalidPageId);
        });

        assertEquals("No page with this ID - " + invalidPageId, exception.getMessage());
    }
    @Test
    void testUnpinPageDoesNotThrowError() {
        Page newPage = bufferManager.createPage();
        int pageId = newPage.getPid();
        assertDoesNotThrow(() -> bufferManager.unpinPage(pageId),
                "unpinPage should not throw an error when page exists in buffer");
    }

}