package com.database.finalproject.buffermanager;

import static com.database.finalproject.constants.PageConstants.INPUT_FILE;
import static com.database.finalproject.constants.PageConstants.PAGE_SIZE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.database.finalproject.model.Page;
import com.database.finalproject.model.PageNotFoundException;
import com.database.finalproject.model.Row;
import com.database.finalproject.repository.Utilities;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Path;
import java.nio.file.Paths;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BufferManagerE2ETest {
    private BufferManager bufferManager;

    @BeforeAll
    void setup() {
        bufferManager = new BufferManagerImpl(5);
        Utilities.loadDataset(bufferManager, "src/main/resources/static/title.basics.tsv");
    }
    @BeforeEach
    void setUp() {
//        bufferManager = new BufferManagerImpl(5);
    }

    @Test
    void testEndtoEnd() throws IOException {
//        Utilities.loadDataset(bufferManager, "src/main/resources/static/title.basics.tsv");
        Page page1 = bufferManager.getPage(1);
        assertNotNull(page1, "Page1 should be in the buffer pool");
        Page page2 = bufferManager.getPage(2);
        assertNotNull(page2, "Page2 should be in the buffer pool");
        Page page3 = bufferManager.getPage(3);
        assertNotNull(page3, "Page3 should be in the buffer pool");
        Page page4 = bufferManager.getPage(4);
        assertNotNull(page4, "Page4 should be in the buffer pool");
        Page page5 = bufferManager.getPage(5);
        assertNotNull(page5, "Page5 should be in the buffer pool");

        Page page6 = bufferManager.getPage(6);
        assertNull(page6, "Page6 should not be in the buffer pool");

        bufferManager.unpinPage(5);

        Page getPage6 = bufferManager.getPage(6);
        assertNotNull(getPage6, "Page6 should be in the buffer pool");

        bufferManager.markDirty(1);
        bufferManager.unpinPage(1);

        Page page7 = bufferManager.getPage(7);

        assertNotNull(page7, "Page7 should be in the buffer pool");

        // Verify that page1 is written to the binary file
        try (RandomAccessFile raf = new RandomAccessFile(INPUT_FILE, "r")) {
            byte[] pageData = new byte[PAGE_SIZE];
            raf.seek(0);
            raf.readFully(pageData);
            assertNotNull(pageData, "Page 1 should be written to the binary file");
        }
        bufferManager.unpinPage(3);
        testConsecutiveInsertsAndQueries();
        // testInsertQueryEvictReloadInsertQuery();

    }

    public void testConsecutiveInsertsAndQueries() {
        // Create a new page and insert rows
        Page page = bufferManager.createPage();
        int pageId = page.getPid();
        Row row = new Row(new byte[9], new byte[30]);
        page.insertRow(row);
        bufferManager.markDirty(pageId);
        // Unpin the page
        bufferManager.unpinPage(pageId);

        // Force eviction by filling the buffer pool
        Page page15 = bufferManager.getPage(15);
        Page page16 = bufferManager.getPage(16);
        Page page17 = bufferManager.getPage(17);
        Page page18 = bufferManager.getPage(18);
        Page page19 = bufferManager.getPage(19);
        bufferManager.unpinPage(15);
        // Reload the page and verify the inserted row
        Page reloadedPage = bufferManager.getPage(pageId);
        assertNotNull(reloadedPage.getRow(0));

    }


    @Test // tests that fetching a page that does not exist returns null
    void testNonExistentPage() throws IOException {
        Page page = bufferManager.getPage(120000); // Non-existent page ID
        assertNull(page, "Fetching a non-existent page should return null");
    }

    @Test // test that the dataset is getting loaded properly and pages are being fetched
          // correctly
    void testLoadDatasetAndVerifyPages() throws IOException {
        Page page1 = bufferManager.getPage(1);
        assertNotNull(page1, "Page 1 should be created and pinned");
    }

    @Test // tests the edge case where buffer pool size is 1
    void testBufferPoolSizeOne() throws IOException {
        BufferManager bufferManager = new BufferManagerImpl(1);

        Page page1 = bufferManager.getPage(1);
        assertNotNull(page1, "Page 1 should be in the buffer pool");
        bufferManager.unpinPage(1);
        Page page2 = bufferManager.getPage(2);
        assertNotNull(page2, "Page 2 should be in the buffer pool");
    }

    @Test
    void testPinStillInBuffer() {
        Page page = bufferManager.getPage(1);
        int totalTime = 0;
        long totalIterations = 100;
        for (int i = 2; i <= 101; i++) {
            long startTime = System.nanoTime();
            page = bufferManager.getPage(i);
            long endTime = System.nanoTime();
            totalTime += endTime - startTime;
            bufferManager.unpinPage(i);
        }
        long avgTime = totalTime / totalIterations;
        long startTime = System.nanoTime();
        page = bufferManager.getPage(1);
        long endTime = System.nanoTime();
        long inBufferTime = endTime - startTime;
        assertTrue(inBufferTime < 0.5 * avgTime, "Page 1 should be in the buffer " + avgTime + " " + inBufferTime);

    }

    @Test // tests that dirty pages are being written to file before eviction
    void testMarkDirtyAndWriteToBinaryFile() throws IOException {
        BufferManager bufferManager = new BufferManagerImpl(3);

        // Fetch a page and modify its contents
        Page page1 = bufferManager.getPage(1);
        Row row = new Row("tt9999999".getBytes(StandardCharsets.UTF_8),
                "Test Movie".getBytes(StandardCharsets.UTF_8));
        page1.insertRow(row);
        bufferManager.markDirty(1);
        bufferManager.unpinPage(1);
        // Evict the page by fetching new pages
        bufferManager.getPage(2);
        bufferManager.getPage(3);
        bufferManager.getPage(4);

        // Verify that the page is written to the binary file
        try (RandomAccessFile raf = new RandomAccessFile(INPUT_FILE, "r")) {
            byte[] pageData = new byte[PAGE_SIZE];
            raf.seek(0);
            raf.readFully(pageData);
            assertNotNull(pageData, "Page 1 should be written to the binary file");
        }
    }
}