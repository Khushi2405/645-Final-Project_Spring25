package com.database.finalproject.buffermanager;

import static com.database.finalproject.constants.PageConstants.*;
import static com.database.finalproject.constants.PageConstants.MOVIES_DATA_PAGE_INDEX;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

import com.database.finalproject.model.DatabaseCatalog;
import com.database.finalproject.model.page.MovieDataPage;
import com.database.finalproject.model.record.MovieRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
        MovieDataPage page1 = (MovieDataPage) bufferManager.getPage(1, MOVIES_DATA_PAGE_INDEX);
        assertNotNull(page1, "Page1 should be in the buffer pool");
        MovieDataPage page2 = (MovieDataPage) bufferManager.getPage(2, MOVIES_DATA_PAGE_INDEX);
        assertNotNull(page2, "Page2 should be in the buffer pool");
        MovieDataPage page3 = (MovieDataPage) bufferManager.getPage(3, MOVIES_DATA_PAGE_INDEX);
        assertNotNull(page3, "Page3 should be in the buffer pool");
        MovieDataPage page4 = (MovieDataPage) bufferManager.getPage(4, MOVIES_DATA_PAGE_INDEX);
        assertNotNull(page4, "Page4 should be in the buffer pool");
        MovieDataPage page5 = (MovieDataPage) bufferManager.getPage(5, MOVIES_DATA_PAGE_INDEX);
        assertNotNull(page5, "Page5 should be in the buffer pool");

        MovieDataPage page6 = (MovieDataPage) bufferManager.getPage(6, MOVIES_DATA_PAGE_INDEX);
        assertNull(page6, "Page6 should not be in the buffer pool");

        bufferManager.unpinPage(5, MOVIES_DATA_PAGE_INDEX);

        MovieDataPage getPage6 = (MovieDataPage) bufferManager.getPage(6, MOVIES_DATA_PAGE_INDEX);
        assertNotNull(getPage6, "Page6 should be in the buffer pool");

        //bufferManager.markDirty(1);
        bufferManager.unpinPage(1, MOVIES_DATA_PAGE_INDEX);

        MovieDataPage page7 = (MovieDataPage) bufferManager.getPage(7, MOVIES_DATA_PAGE_INDEX);

        assertNotNull(page7, "Page7 should be in the buffer pool");

        // Verify that page1 is written to the binary file

        bufferManager.unpinPage(2, MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(3, MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(4, MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(6, MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(7, MOVIES_DATA_PAGE_INDEX);
        testConsecutiveInsertsAndQueries();
        testInsertQueryEvictReloadInsertQuery();
        testInsertQueryMarkDirtyEvictReload();
    }

    public void testConsecutiveInsertsAndQueries() {
        // Create a new page and insert rows
        MovieDataPage page = (MovieDataPage) bufferManager.createPage();
        int pageId = page.getPid();
        MovieRecord movieRecord1 = new MovieRecord("tt0000001".getBytes(), "Movie One".getBytes());
        page.insertRecord(movieRecord1);
        // Unpin the page
        bufferManager.unpinPage(pageId);

        // Force eviction by filling the buffer pool
        MovieDataPage page15 = (MovieDataPage) bufferManager.getPage(15, MOVIES_DATA_PAGE_INDEX);
        MovieDataPage page16 = (MovieDataPage) bufferManager.getPage(16, MOVIES_DATA_PAGE_INDEX);
        MovieDataPage page17 = (MovieDataPage) bufferManager.getPage(17, MOVIES_DATA_PAGE_INDEX);
        MovieDataPage page18 = (MovieDataPage) bufferManager.getPage(18, MOVIES_DATA_PAGE_INDEX);
        MovieDataPage page19 = (MovieDataPage) bufferManager.getPage(19, MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(page15.getPid(), MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(page16.getPid(), MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(page17.getPid(), MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(page18.getPid(), MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(page19.getPid(), MOVIES_DATA_PAGE_INDEX);

        // Reload the page and verify the inserted row
        MovieDataPage reloadedPage = (MovieDataPage) bufferManager.getPage(pageId, MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(reloadedPage.getPid(), MOVIES_DATA_PAGE_INDEX);
        assertEquals(reloadedPage.getPid(), pageId);
        assertEquals("MovieRecord{movieId=tt0000001, title=Movie One}", reloadedPage.getRecord(0).toString());

    }

    public void testInsertQueryEvictReloadInsertQuery() {
        // Create a new page
        MovieDataPage page = (MovieDataPage) bufferManager.createPage();
        int pageId = page.getPid();

        // Insert 2 rows
        MovieRecord movieRecord1 = new MovieRecord("tt0000001".getBytes(), "Movie One".getBytes());
        MovieRecord movieRecord2 = new MovieRecord("tt0000002".getBytes(), "Movie Two".getBytes());
        page.insertRecord(movieRecord1);
        page.insertRecord(movieRecord2);

        // Query the rows
        assertEquals("MovieRecord{movieId=tt0000001, title=Movie One}", page.getRecord(0).toString());
        assertEquals("MovieRecord{movieId=tt0000002, title=Movie Two}", page.getRecord(1).toString());

        // Unpin the page
        bufferManager.unpinPage(pageId);

        // Fill the buffer pool to force eviction
        MovieDataPage page15 = (MovieDataPage) bufferManager.getPage(15, MOVIES_DATA_PAGE_INDEX);
        MovieDataPage page16 = (MovieDataPage) bufferManager.getPage(16, MOVIES_DATA_PAGE_INDEX);
        MovieDataPage page17 = (MovieDataPage) bufferManager.getPage(17, MOVIES_DATA_PAGE_INDEX);
        MovieDataPage page18 = (MovieDataPage) bufferManager.getPage(18, MOVIES_DATA_PAGE_INDEX);
        MovieDataPage page19 = (MovieDataPage) bufferManager.getPage(19, MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(page15.getPid(), MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(page16.getPid(), MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(page17.getPid(), MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(page18.getPid(), MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(page19.getPid(), MOVIES_DATA_PAGE_INDEX);

        // Reload the page and insert 2 more rows
        page = (MovieDataPage) bufferManager.getPage(pageId, MOVIES_DATA_PAGE_INDEX);
        MovieRecord movieRecord3 = new MovieRecord("tt0000003".getBytes(), "Movie Three".getBytes());
        MovieRecord movieRecord4 = new MovieRecord("tt0000004".getBytes(), "Movie Four".getBytes());
        page.insertRecord(movieRecord3);
        page.insertRecord(movieRecord4);

        // Query all rows
        assertEquals("MovieRecord{movieId=tt0000001, title=Movie One}", page.getRecord(0).toString());
        assertEquals("MovieRecord{movieId=tt0000002, title=Movie Two}", page.getRecord(1).toString());
        assertEquals("MovieRecord{movieId=tt0000003, title=Movie Three}", page.getRecord(2).toString());
        assertEquals("MovieRecord{movieId=tt0000004, title=Movie Four}", page.getRecord(3).toString());

        // Unpin the page
        bufferManager.unpinPage(pageId);
    }

    public void testInsertQueryMarkDirtyEvictReload() {
        // Create a new page
        MovieDataPage page = (MovieDataPage) bufferManager.createPage();
        int pageId = page.getPid();

        // Insert 2 rows
        MovieRecord movieRecord1 = new MovieRecord("tt0000001".getBytes(), "Movie One".getBytes());
        MovieRecord movieRecord2 = new MovieRecord("tt0000002".getBytes(), "Movie Two".getBytes());
        page.insertRecord(movieRecord1);
        page.insertRecord(movieRecord2);

        // Query the rows
        assertEquals("MovieRecord{movieId=tt0000001, title=Movie One}", page.getRecord(0).toString());
        assertEquals("MovieRecord{movieId=tt0000002, title=Movie Two}", page.getRecord(1).toString());

        // Unpin the page
        bufferManager.unpinPage(pageId);

        // Fill the buffer pool to force eviction

        MovieDataPage page15 = (MovieDataPage) bufferManager.getPage(15, MOVIES_DATA_PAGE_INDEX);
        MovieDataPage page16 = (MovieDataPage) bufferManager.getPage(16, MOVIES_DATA_PAGE_INDEX);
        MovieDataPage page17 = (MovieDataPage) bufferManager.getPage(17, MOVIES_DATA_PAGE_INDEX);
        MovieDataPage page18 = (MovieDataPage) bufferManager.getPage(18, MOVIES_DATA_PAGE_INDEX);
        MovieDataPage page19 = (MovieDataPage) bufferManager.getPage(19, MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(page15.getPid(), MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(page16.getPid(), MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(page17.getPid(), MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(page18.getPid(), MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(page19.getPid(), MOVIES_DATA_PAGE_INDEX);

        // Reload the page and query the rows
        MovieDataPage reloadedPage = (MovieDataPage) bufferManager.getPage(pageId, MOVIES_DATA_PAGE_INDEX);
        assertEquals("MovieRecord{movieId=tt0000001, title=Movie One}", reloadedPage.getRecord(0).toString());
        assertEquals("MovieRecord{movieId=tt0000002, title=Movie Two}", reloadedPage.getRecord(1).toString());
        bufferManager.unpinPage(reloadedPage.getPid(), MOVIES_DATA_PAGE_INDEX);

    }

    @Test // tests that fetching a page that does not exist returns null
    void testNonExistentPage(){
        MovieDataPage page = (MovieDataPage) bufferManager.getPage(120000, MOVIES_DATA_PAGE_INDEX); // Non-existent page ID
        assertNull(page, "Fetching a non-existent page should return null");
    }

    @Test // test that the dataset is getting loaded properly and pages are being fetched
          // correctly
    void testLoadDatasetAndVerifyPages(){
        MovieDataPage page1 = (MovieDataPage) bufferManager.getPage(1, MOVIES_DATA_PAGE_INDEX);
        assertNotNull(page1, "MovieDataPage 1 should be pinned");
        bufferManager.unpinPage(page1.getPid(), MOVIES_DATA_PAGE_INDEX);
    }

    @Test
    void testPinStillInBuffer() {
        MovieDataPage page = (MovieDataPage) bufferManager.getPage(1);
        int totalTime = 0;
        long totalIterations = 100;
        for (int i = 2; i <= 101; i++) {
            long startTime = System.nanoTime();
            page = (MovieDataPage) bufferManager.getPage(i, MOVIES_DATA_PAGE_INDEX);
            long endTime = System.nanoTime();
            totalTime += endTime - startTime;
            bufferManager.unpinPage(i, MOVIES_DATA_PAGE_INDEX);
        }
        long avgTime = totalTime / totalIterations;
        long startTime = System.nanoTime();
        page = (MovieDataPage) bufferManager.getPage(1, MOVIES_DATA_PAGE_INDEX);
        long endTime = System.nanoTime();
        long inBufferTime = endTime - startTime;
        assertTrue(inBufferTime < avgTime, "MovieDataPage 1 should be in the buffer " + avgTime + " " + inBufferTime);
        bufferManager.unpinPage(page.getPid(), MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(page.getPid(), MOVIES_DATA_PAGE_INDEX);
    }

    @Test // tests that dirty pages are being written to file before eviction
    void testMarkDirtyAndWriteToBinaryFile() {
//        BufferManager bufferManager = new BufferManagerImpl(3);

        // Fetch a page and modify its contents
        MovieDataPage newpage = (MovieDataPage) bufferManager.createPage(MOVIES_DATA_PAGE_INDEX);
        int newPageId = newpage.getPid();
//        MovieDataPage page1 = bufferManager.getPage(1);
        MovieRecord movieRecord = new MovieRecord("tt9999999".getBytes(StandardCharsets.UTF_8),
                "Test Movie".getBytes(StandardCharsets.UTF_8));
        newpage.insertRecord(movieRecord);
        //bufferManager.markDirty(newPageId);
        bufferManager.unpinPage(newPageId);
        // Evict the page by fetching new pages
        bufferManager.getPage(2, MOVIES_DATA_PAGE_INDEX);
        bufferManager.getPage(3, MOVIES_DATA_PAGE_INDEX);
        bufferManager.getPage(4, MOVIES_DATA_PAGE_INDEX);
        bufferManager.getPage(5, MOVIES_DATA_PAGE_INDEX);
        bufferManager.getPage(6, MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(2, MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(3, MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(4, MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(5, MOVIES_DATA_PAGE_INDEX);
        bufferManager.unpinPage(6, MOVIES_DATA_PAGE_INDEX);


        // Verify that the page is written to the binary file
        DatabaseCatalog catalog = new DatabaseCatalog("src/main/resources/static/database_catalog.txt");
        try (RandomAccessFile raf = new RandomAccessFile(catalog.getCatalog(WORKED_ON_DATA_PAGE_INDEX).get("filename"), "r")) {
            byte[] pageData = new byte[PAGE_SIZE];
            raf.seek((long) (newPageId) * PAGE_SIZE);
            raf.readFully(pageData);
            assertNotNull(pageData, "MovieDataPage should be written to the binary file");
        }
        catch (IOException e){
            System.out.println("Exception occurred " + e);
        }
    }
}