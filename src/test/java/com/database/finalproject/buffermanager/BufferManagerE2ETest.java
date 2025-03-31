package com.database.finalproject.buffermanager;

import static com.database.finalproject.constants.PageConstants.DATA_INPUT_FILE;
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

import com.database.finalproject.model.DataPage;
import com.database.finalproject.model.Row;
import com.database.finalproject.repository.Utilities;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BufferManagerE2ETest {
    private BufferManager bufferManager;

    @BeforeAll
    void setup() {
        bufferManager = new BufferManagerImpl(5);
//        Utilities.loadDataset(bufferManager, "src/main/resources/static/title.basics.tsv");
    }

    @BeforeEach
    void setUp() {
        // bufferManager = new BufferManagerImpl(5);
    }

    @Test
    void testBufferManagerCapacity() {
        // Utilities.loadDataset(bufferManager,
        // "src/main/resources/static/title.basics.tsv");
        DataPage page1 = (DataPage) bufferManager.getPage(1);
        assertNotNull(page1, "Page1 should be in the buffer pool");
        DataPage page2 = (DataPage) bufferManager.getPage(2);
        assertNotNull(page2, "Page2 should be in the buffer pool");
        DataPage page3 = (DataPage) bufferManager.getPage(3);
        assertNotNull(page3, "Page3 should be in the buffer pool");
        DataPage page4 = (DataPage) bufferManager.getPage(4);
        assertNotNull(page4, "Page4 should be in the buffer pool");
        DataPage page5 = (DataPage) bufferManager.getPage(5);
        assertNotNull(page5, "Page5 should be in the buffer pool");

        DataPage page6 = (DataPage) bufferManager.getPage(6);
        assertNull(page6, "Page6 should not be in the buffer pool");

        bufferManager.unpinPage(5);

        DataPage getPage6 = (DataPage) bufferManager.getPage(6);
        assertNotNull(getPage6, "Page6 should be in the buffer pool");

        //bufferManager.markDirty(1);
        bufferManager.unpinPage(1);

        DataPage page7 = (DataPage) bufferManager.getPage(7);

        assertNotNull(page7, "Page7 should be in the buffer pool");

        // Verify that page1 is written to the binary file

        bufferManager.unpinPage(2);
        bufferManager.unpinPage(3);
        bufferManager.unpinPage(4);
        bufferManager.unpinPage(6);
        bufferManager.unpinPage(7);
        testConsecutiveInsertsAndQueries();
        testInsertQueryEvictReloadInsertQuery();
        testInsertQueryMarkDirtyEvictReload();
    }

    public void testConsecutiveInsertsAndQueries() {
        // Create a new page and insert rows
        DataPage page = (DataPage) bufferManager.createPage();
        int pageId = page.getPid();
        Row row1 = new Row("tt0000001".getBytes(), "Movie One".getBytes());
        page.insertRow(row1);
        // Unpin the page
        bufferManager.unpinPage(pageId);

        // Force eviction by filling the buffer pool
        DataPage page15 = (DataPage) bufferManager.getPage(15);
        DataPage page16 = (DataPage) bufferManager.getPage(16);
        DataPage page17 = (DataPage) bufferManager.getPage(17);
        DataPage page18 = (DataPage) bufferManager.getPage(18);
        DataPage page19 = (DataPage) bufferManager.getPage(19);
        bufferManager.unpinPage(page15.getPid());
        bufferManager.unpinPage(page16.getPid());
        bufferManager.unpinPage(page17.getPid());
        bufferManager.unpinPage(page18.getPid());
        bufferManager.unpinPage(page19.getPid());

        // Reload the page and verify the inserted row
        DataPage reloadedPage = (DataPage) bufferManager.getPage(pageId);
        bufferManager.unpinPage(reloadedPage.getPid());
        assertEquals(reloadedPage.getPid(), pageId);
        assertEquals("Row{movieId=tt0000001, title=Movie One}", reloadedPage.getRow(0).toString());

    }

    public void testInsertQueryEvictReloadInsertQuery() {
        // Create a new page
        DataPage page = (DataPage) bufferManager.createPage();
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
        DataPage page15 = (DataPage) bufferManager.getPage(15);
        DataPage page16 = (DataPage) bufferManager.getPage(16);
        DataPage page17 = (DataPage) bufferManager.getPage(17);
        DataPage page18 = (DataPage) bufferManager.getPage(18);
        DataPage page19 = (DataPage) bufferManager.getPage(19);
        bufferManager.unpinPage(page15.getPid());
        bufferManager.unpinPage(page16.getPid());
        bufferManager.unpinPage(page17.getPid());
        bufferManager.unpinPage(page18.getPid());
        bufferManager.unpinPage(page19.getPid());

        // Reload the page and insert 2 more rows
        page = (DataPage) bufferManager.getPage(pageId);
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
        DataPage page = (DataPage) bufferManager.createPage();
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

        DataPage page15 = (DataPage) bufferManager.getPage(15);
        DataPage page16 = (DataPage) bufferManager.getPage(16);
        DataPage page17 = (DataPage) bufferManager.getPage(17);
        DataPage page18 = (DataPage) bufferManager.getPage(18);
        DataPage page19 = (DataPage) bufferManager.getPage(19);
        bufferManager.unpinPage(page15.getPid());
        bufferManager.unpinPage(page16.getPid());
        bufferManager.unpinPage(page17.getPid());
        bufferManager.unpinPage(page18.getPid());
        bufferManager.unpinPage(page19.getPid());

        // Reload the page and query the rows
        DataPage reloadedPage = (DataPage) bufferManager.getPage(pageId);
        assertEquals("Row{movieId=tt0000001, title=Movie One}", reloadedPage.getRow(0).toString());
        assertEquals("Row{movieId=tt0000002, title=Movie Two}", reloadedPage.getRow(1).toString());
        bufferManager.unpinPage(reloadedPage.getPid());

    }

    @Test // tests that fetching a page that does not exist returns null
    void testNonExistentPage(){
        DataPage page = (DataPage) bufferManager.getPage(120000); // Non-existent page ID
        assertNull(page, "Fetching a non-existent page should return null");
    }

    @Test // test that the dataset is getting loaded properly and pages are being fetched
          // correctly
    void testLoadDatasetAndVerifyPages(){
        DataPage page1 = (DataPage) bufferManager.getPage(1);
        assertNotNull(page1, "DataPage 1 should be pinned");
        bufferManager.unpinPage(page1.getPid());
    }

    @Test
    void testPinStillInBuffer() {
        DataPage page = (DataPage) bufferManager.getPage(1);
        int totalTime = 0;
        long totalIterations = 100;
        for (int i = 2; i <= 101; i++) {
            long startTime = System.nanoTime();
            page = (DataPage) bufferManager.getPage(i);
            long endTime = System.nanoTime();
            totalTime += endTime - startTime;
            bufferManager.unpinPage(i);
        }
        long avgTime = totalTime / totalIterations;
        long startTime = System.nanoTime();
        page = (DataPage) bufferManager.getPage(1);
        long endTime = System.nanoTime();
        long inBufferTime = endTime - startTime;
        assertTrue(inBufferTime < avgTime, "DataPage 1 should be in the buffer " + avgTime + " " + inBufferTime);
        bufferManager.unpinPage(page.getPid());
        bufferManager.unpinPage(page.getPid());
    }

    @Test // tests that dirty pages are being written to file before eviction
    void testMarkDirtyAndWriteToBinaryFile() {
//        BufferManager bufferManager = new BufferManagerImpl(3);

        // Fetch a page and modify its contents
        DataPage newpage = (DataPage) bufferManager.createPage();
        int newPageId = newpage.getPid();
//        DataPage page1 = bufferManager.getPage(1);
        Row row = new Row("tt9999999".getBytes(StandardCharsets.UTF_8),
                "Test Movie".getBytes(StandardCharsets.UTF_8));
        newpage.insertRow(row);
        //bufferManager.markDirty(newPageId);
        bufferManager.unpinPage(newPageId);
        // Evict the page by fetching new pages
        bufferManager.getPage(2);
        bufferManager.getPage(3);
        bufferManager.getPage(4);
        bufferManager.getPage(5);
        bufferManager.getPage(6);
        bufferManager.unpinPage(2);
        bufferManager.unpinPage(3);
        bufferManager.unpinPage(4);
        bufferManager.unpinPage(5);
        bufferManager.unpinPage(6);


        // Verify that the page is written to the binary file
        try (RandomAccessFile raf = new RandomAccessFile(DATA_INPUT_FILE, "r")) {
            byte[] pageData = new byte[PAGE_SIZE];
            raf.seek((long) (newPageId) * PAGE_SIZE);
            raf.readFully(pageData);
            assertNotNull(pageData, "DataPage should be written to the binary file");
        }
        catch (IOException e){
            System.out.println("Exception occurred " + e);
        }
    }
}