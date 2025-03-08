package com.database.finalproject.buffermanager;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.database.finalproject.model.Page;
import com.database.finalproject.model.Row;

class BufferManagerImplTest {
    private BufferManager bufferManager;

    @BeforeEach
    void setUp() {
        bufferManager = new BufferManagerImpl(5);
    }

    // page
    @Test
    void testCreatePage() {
        Page page = bufferManager.createPage();
        assertNotNull(page, "Page should be created successfully");
        assertTrue(page.getPid() >= 0, "Page ID should be valid");
    }

    @Test
    void testFetchPage() {
        Page createdPage = bufferManager.createPage();
        int pageId = createdPage.getPid();

        Page fetchedPage = bufferManager.getPage(pageId);
        assertNotNull(fetchedPage, "Fetched page should not be null");
        assertEquals(pageId, fetchedPage.getPid(), "Fetched page ID should match");
    }

    @Test
    void testBufferFullForCreatePage() {
        BufferManagerImpl bufferManagerSpy = spy(new BufferManagerImpl(5));
        for (int i = 1; i <= 5; i++) {
            Page createdPage = bufferManagerSpy.createPage();
        }
        Page createdPageExtra = bufferManagerSpy.createPage();

        assertNull(createdPageExtra, "Page should not be create, response should be NULL");
    }

    @Test
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

    @Test
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

    @Test
    void testMarkDirtyOnCreatePage() {
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

        // Verify that page2 is evicted
        assertNull(bufferManager.getPage(page2.getPid()), "Page2 should be evicted");
        assertNotNull(bufferManager.getPage(page1.getPid()), "Page1 should still be in the buffer");
        assertNotNull(bufferManager.getPage(newPage.getPid()), "New page should be in the buffer");
    }

    // rows
    @Test
    void testInsertRow() {
        Page page = bufferManager.createPage();
        Row row = new Row("tt1111111".getBytes(StandardCharsets.UTF_8),
                "Test Movie Insert".getBytes(StandardCharsets.UTF_8));

        int rowId = page.insertRow(row) - 1;
        assertEquals(0, rowId, "Row should be inserted at index 0");

        Row fetchedRow = page.getRow(rowId);
        assertNotNull(fetchedRow, "Inserted row should be retrievable");
    }

}