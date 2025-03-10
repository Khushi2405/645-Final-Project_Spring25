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
        // bufferManager = new BufferManagerImpl(5);
    }

    @Test
    void testEndtoEnd() {
        // Utilities.loadDataset(bufferManager,
        // "src/main/resources/static/title.basics.tsv");
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
        catch (IOException e){

        }
        bufferManager.unpinPage(3);
        testConsecutiveInsertsAndQueries();
        testInsertQueryEvictReloadInsertQuery();
        testInsertQueryMarkDirtyEvictReload();
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

    public void testInsertQueryEvictReloadInsertQuery() {
        // Create a new page
        bufferManager.unpinPage(4);
        Page page = bufferManager.createPage();
        int pageId = page.getPid();

        // Insert 2 rows
        Row row1 = new Row("tt0000001".getBytes(), "Movie One".getBytes());
        Row row2 = new Row("tt0000002".getBytes(), "Movie Two".getBytes());
        page.insertRow(row1);
        page.insertRow(row2);

        // Query the rows
        assertEquals("Row{movieId=tt0000001, title=Movie One}", page.getRow(0).toString());
        assertEquals("Row{movieId=tt0000002, title=Movie Two}", page.getRow(1).toString());

        // Unpin the page
        bufferManager.unpinPage(pageId);

        // Fill the buffer pool to force eviction

        Page page15 = bufferManager.getPage(15);
        Page page16 = bufferManager.getPage(16);
        Page page17 = bufferManager.getPage(17);
        Page page18 = bufferManager.getPage(18);
        Page page19 = bufferManager.getPage(19);
        bufferManager.unpinPage(15);
        // Reload the page and insert 2 more rows
        page = bufferManager.getPage(pageId);
        Row row3 = new Row("tt0000003".getBytes(), "Movie Three".getBytes());
        Row row4 = new Row("tt0000004".getBytes(), "Movie Four".getBytes());
        page.insertRow(row3);
        page.insertRow(row4);

        // Query all rows
        assertEquals("Row{movieId=tt0000001, title=Movie One}", page.getRow(0).toString());
        assertEquals("Row{movieId=tt0000002, title=Movie Two}", page.getRow(1).toString());
        assertEquals("Row{movieId=tt0000003, title=Movie Three}", page.getRow(2).toString());
        assertEquals("Row{movieId=tt0000004, title=Movie Four}", page.getRow(3).toString());

        // Unpin the page
        bufferManager.unpinPage(pageId);
    }

    public void testInsertQueryMarkDirtyEvictReload() {
        // Create a new page
        Page page = bufferManager.createPage();
        int pageId = page.getPid();

        // Insert 2 rows
        Row row1 = new Row("tt0000001".getBytes(), "Movie One".getBytes());
        Row row2 = new Row("tt0000002".getBytes(), "Movie Two".getBytes());
        page.insertRow(row1);
        page.insertRow(row2);

        // Query the rows
        assertEquals("Row{movieId=tt0000001, title=Movie One}", page.getRow(0).toString());
        assertEquals("Row{movieId=tt0000002, title=Movie Two}", page.getRow(1).toString());

        // Mark the page as dirty
        bufferManager.markDirty(pageId);

        // Unpin the page
        bufferManager.unpinPage(pageId);

        // Fill the buffer pool to force eviction

        Page page15 = bufferManager.getPage(15);
        Page page16 = bufferManager.getPage(16);
        Page page17 = bufferManager.getPage(17);
        Page page18 = bufferManager.getPage(18);
        Page page19 = bufferManager.getPage(19);
        bufferManager.unpinPage(15);

        // Reload the page and query the rows
        Page reloadedPage = bufferManager.getPage(pageId);
        assertEquals("Row{movieId=tt0000001, title=Movie One}", reloadedPage.getRow(0).toString());
        assertEquals("Row{movieId=tt0000002, title=Movie Two}", reloadedPage.getRow(1).toString());

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
        assertNotNull(page1, "Page 1 should be pinned");
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
        assertTrue(inBufferTime < avgTime, "Page 1 should be in the buffer " + avgTime + " " + inBufferTime);

    }

    @Test // tests that dirty pages are being written to file before eviction
    void testMarkDirtyAndWriteToBinaryFile() throws IOException {
//        BufferManager bufferManager = new BufferManagerImpl(3);

        // Fetch a page and modify its contents
        Page newpage = bufferManager.createPage();
        int newPageId = newpage.getPid();
//        Page page1 = bufferManager.getPage(1);
        Row row = new Row("tt9999999".getBytes(StandardCharsets.UTF_8),
                "Test Movie".getBytes(StandardCharsets.UTF_8));
        newpage.insertRow(row);
        bufferManager.markDirty(newPageId);
        bufferManager.unpinPage(newPageId);
        // Evict the page by fetching new pages
        bufferManager.getPage(2);
        bufferManager.getPage(3);
        bufferManager.getPage(4);

        // Verify that the page is written to the binary file
        try (RandomAccessFile raf = new RandomAccessFile(INPUT_FILE, "r")) {
            byte[] pageData = new byte[PAGE_SIZE];
            raf.seek((long) (newPageId) * PAGE_SIZE);
            raf.readFully(pageData);
            assertNotNull(pageData, "Page should be written to the binary file");
        }
    }
}