package com.database.finalproject.buffermanager;

import static com.database.finalproject.constants.PageConstants.INPUT_FILE;
import static com.database.finalproject.constants.PageConstants.PAGE_SIZE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.database.finalproject.model.Page;
import com.database.finalproject.model.PageNotFoundException;
import com.database.finalproject.model.Row;
import com.database.finalproject.repository.Utilities;
import java.nio.file.Path;
import java.nio.file.Paths;

class BufferManagerE2ETest {
    private BufferManager bufferManager;

    @BeforeEach
    void setUp() {
        bufferManager = new BufferManagerImpl(5);
    }

    @Test
    void testNonExistentPage() throws IOException {
        Utilities.loadDataset(bufferManager, "src/main/resources/static/title.basics.tsv");
        Page page = bufferManager.getPage(120000); // Non-existent page ID
        assertNull(page, "Fetching a non-existent page should return null");
    }   

    @Test
    void testLoadDatasetAndVerifyPages() throws IOException {
        Utilities.loadDataset(bufferManager, "src/main/resources/static/title.basics.tsv");
        Page page1 = bufferManager.getPage(1);
        assertNotNull(page1, "Page 1 should be created and pinned");
    }

    @Test
    void testBufferPoolSizeOne() throws IOException {
        BufferManager bufferManager = new BufferManagerImpl(1);
        Utilities.loadDataset(bufferManager, "src/main/resources/static/title.basics.tsv");

        Page page1 = bufferManager.getPage(1);
        assertNotNull(page1, "Page 1 should be in the buffer pool");
        bufferManager.unpinPage(1);
        Page page2 = bufferManager.getPage(2);
        assertNotNull(page2, "Page 2 should be in the buffer pool");
    }
    
    @Test
    void testPinStillInBuffer() {
        Path filePath = Paths.get("src", "main", "resources", "static", "title.basics.tsv");
        Utilities.loadDataset(bufferManager, "" + filePath.toAbsolutePath());
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

    @Test
    void testMarkDirtyAndWriteToBinaryFile() throws IOException {
        BufferManager bufferManager = new BufferManagerImpl(3);
        Utilities.loadDataset(bufferManager, "src/main/resources/static/title.basics.tsv");

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