package com.database.finalproject.buffermanager;

import static com.database.finalproject.constants.PageConstants.MOVIES_DATA_PAGE_INDEX;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;

import com.database.finalproject.model.page.MovieDataPage;
import com.database.finalproject.model.record.MovieRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;

class BufferManagerImplTest {
    private BufferManagerImpl bufferManager;
    private Logger mockLogger;

    @BeforeEach
    void setUp() {
        bufferManager = new BufferManagerImpl(5);
        mockLogger = mock(Logger.class);
        bufferManager.logger = mockLogger;
    }

    // page
    // createPage tests
    // @Test
    // void testCreatePage() {
    // // create a new page
    // MovieDataPage page = (MovieDataPage) bufferManager.createPage();
    // assertNotNull(page, "Page should be created successfully");
    // assertTrue(page.getPid() >= 0, "Page ID should be valid");
    // }

    // @Test
    // void testBufferFullForCreatePage() {
    // // buffer full for createPage
    // BufferManagerImpl bufferManagerSpy = spy(new BufferManagerImpl(5));
    // for (int i = 1; i <= 5; i++) {
    // MovieDataPage createdPage = (MovieDataPage) bufferManagerSpy.createPage();
    // }
    // MovieDataPage createdPageExtra = (MovieDataPage)
    // bufferManagerSpy.createPage();

    // assertNull(createdPageExtra, "Page should not be create, response should be
    // NULL");
    // }

    // // getPage tests
    // @Test
    // void testFetchPage() {
    // // page already in buffer pool
    // MovieDataPage createdPage = (MovieDataPage) bufferManager.createPage();
    // int pageId = createdPage.getPid();

    // MovieDataPage fetchedPage = (MovieDataPage) bufferManager.getPage(pageId);
    // assertNotNull(fetchedPage, "Fetched page should not be null");
    // assertEquals(pageId, fetchedPage.getPid(), "Fetched page ID should match");
    // }

    // @Test
    // void testFetchPageNotInBuffer() {
    // // page not in buffer pool
    // for (int i = 1; i <= 4; i++) {
    // MovieDataPage createdPage = (MovieDataPage) bufferManager.createPage();
    // }
    // MovieDataPage page5 = (MovieDataPage) bufferManager.createPage();
    // int pageId5 = page5.getPid();
    // bufferManager.unpinPage(pageId5);

    // // evict page 5
    // MovieDataPage page6 = (MovieDataPage) bufferManager.createPage();
    // int pageId6 = page6.getPid();
    // bufferManager.unpinPage(pageId6);

    // // get page 5, evict 6
    // MovieDataPage fetchedPage = (MovieDataPage) bufferManager.getPage(pageId5);
    // assertNotNull(fetchedPage, "Fetched page should not be null");
    // assertEquals(pageId5, fetchedPage.getPid(), "Fetched page ID should match");
    // }

    // @Test
    // void testBufferFullForGetPage() {
    // // buffer full on getPage
    // BufferManagerImpl bufferManagerSpy = spy(new BufferManagerImpl(5));
    // for (int i = 1; i <= 4; i++) {
    // MovieDataPage createdPage = (MovieDataPage) bufferManagerSpy.createPage();
    // }
    // MovieDataPage page5 = (MovieDataPage) bufferManagerSpy.createPage();
    // int pageId5 = page5.getPid();
    // bufferManagerSpy.unpinPage(pageId5);

    // MovieDataPage newPage = (MovieDataPage) bufferManagerSpy.createPage();

    // MovieDataPage getPage5 = (MovieDataPage) bufferManagerSpy.getPage(pageId5);

    // assertNull(getPage5, "Buffer full: Cannot get page, response should be
    // NULL");
    // }

    // @Test
    // void testWritePageCall() {
    // // empty page writes
    // BufferManagerImpl bufferManagerSpy = spy(new BufferManagerImpl(5));
    // for (int i = 1; i <= 4; i++) {
    // MovieDataPage createdPage = (MovieDataPage) bufferManagerSpy.createPage();
    // }
    // MovieDataPage page5 = (MovieDataPage) bufferManagerSpy.createPage();
    // int pageId5 = page5.getPid();
    // bufferManagerSpy.unpinPage(pageId5);

    // MovieDataPage newPage = (MovieDataPage) bufferManagerSpy.createPage();
    // verify(bufferManagerSpy).writeToBinaryFile(page5, MOVIES_DATA_PAGE_INDEX);
    // }

    // @Test
    // void testMarkDirtyOnCreatePage() {
    // // page to be marked dirty on createPage
    // BufferManagerImpl bufferManagerSpy = spy(new BufferManagerImpl(5));

    // MovieDataPage newPage = (MovieDataPage) bufferManagerSpy.createPage();
    // int pageId = newPage.getPid();

    // // Verify if markDirty(pageId) was called inside createPage()
    // verify(bufferManagerSpy).markDirty(pageId);
    // }

    // // rows
    // @Test
    // void testInsertRow() {
    // MovieDataPage page = (MovieDataPage) bufferManager.createPage();
    // MovieRecord movieRecord = new
    // MovieRecord("tt1111111".getBytes(StandardCharsets.UTF_8),
    // "Test Movie Insert".getBytes(StandardCharsets.UTF_8));

    // int rowId = page.insertRecord(movieRecord);
    // assertEquals(0, rowId, "MovieRecord should be inserted at index 0");

    // MovieRecord fetchedMovieRecord = page.getRecord(rowId);
    // assertNotNull(fetchedMovieRecord, "Inserted movieRecord should be
    // retrievable");

    // assertFalse(page.isFull(), "Page is not full.");
    // }

    // @Test
    // void testPageFull() {
    // MovieDataPage page = (MovieDataPage) bufferManager.createPage();
    // for (int i = 1; i <= 105; i++) {
    // MovieRecord movieRecord = new
    // MovieRecord("tt1111111".getBytes(StandardCharsets.UTF_8),
    // "Test Movie Insert".getBytes(StandardCharsets.UTF_8));
    // int rowId = page.insertRecord(movieRecord);
    // }

    // assertTrue(page.isFull(), "Page is full.");

    // MovieRecord movieRecord = new
    // MovieRecord("tt1111111".getBytes(StandardCharsets.UTF_8),
    // "Test Movie Insert".getBytes(StandardCharsets.UTF_8));
    // int rowId = page.insertRecord(movieRecord);
    // assertEquals(-1, rowId, "Should return -1 on new insertion if page is full");
    // }

    // @Test
    // void testWriteToBinaryFileInsertRow() {
    // // write to binary file when page with rows inserted and marked dirty is
    // evicted
    // BufferManagerImpl bufferManagerSpy = spy(new BufferManagerImpl(5));
    // // make 4 pages and pin
    // for (int i = 1; i <= 4; i++) {
    // MovieDataPage createdPage = (MovieDataPage) bufferManagerSpy.createPage();
    // }
    // // create 5th page and unpin
    // MovieDataPage page5 = (MovieDataPage) bufferManagerSpy.createPage();
    // int pageId5 = page5.getPid();
    // bufferManagerSpy.unpinPage(pageId5);

    // // create 6th page, page 5 will be evicted and unpin page 6
    // MovieDataPage page6 = (MovieDataPage) bufferManagerSpy.createPage();
    // int pageId6 = page6.getPid();
    // bufferManagerSpy.unpinPage(pageId6);

    // // get page5 [page6 gets evicted], insert rows, mark dirty, unpin
    // MovieDataPage getPage5 = (MovieDataPage) bufferManagerSpy.getPage(pageId5);
    // MovieRecord movieRecord = new
    // MovieRecord("tt1111111".getBytes(StandardCharsets.UTF_8),
    // "Test Movie Insert".getBytes(StandardCharsets.UTF_8));
    // int rowId = getPage5.insertRecord(movieRecord) - 1;
    // bufferManagerSpy.markDirty(pageId5);
    // bufferManagerSpy.unpinPage(pageId5);

    // // get page6 [5 gets evicted, should call writeToBinaryFile]
    // MovieDataPage getPage6 = (MovieDataPage) bufferManagerSpy.getPage(pageId6);
    // verify(bufferManagerSpy).writeToBinaryFile(getPage5, MOVIES_DATA_PAGE_INDEX);
    // }

    // // markDirty exception
    // @Test
    // void testMarkDirtyWhenPageNotFound() {
    // int invalidPageId = 999;

    // bufferManager.markDirty(invalidPageId);
    // ArgumentCaptor<String> logMessageCaptor =
    // ArgumentCaptor.forClass(String.class);
    // verify(mockLogger).error(logMessageCaptor.capture(), eq(invalidPageId));

    // assertEquals("Page not found: {}", logMessageCaptor.getValue());
    // }

    // @Test
    // void testMarkDirtyDoesNotThrowError() {
    // MovieDataPage newPage = (MovieDataPage) bufferManager.createPage();
    // int pageId = newPage.getPid();
    // bufferManager.markDirty(pageId);

    // verify(mockLogger, never()).error(anyString(), anyInt());
    // }

    // // unpin Exception
    // @Test
    // void testUnpinPageThrowsExceptionWhenPageNotFound() {
    // int invalidPageId = 999;

    // bufferManager.unpinPage(invalidPageId);
    // ArgumentCaptor<String> logMessageCaptor =
    // ArgumentCaptor.forClass(String.class);
    // verify(mockLogger).error(logMessageCaptor.capture(), eq(invalidPageId));

    // assertEquals("Page not found: {}", logMessageCaptor.getValue());
    // }

    // @Test
    // void testUnpinPageDoesNotThrowError() {
    // MovieDataPage newPage = (MovieDataPage) bufferManager.createPage();
    // int pageId = newPage.getPid();

    // bufferManager.unpinPage(pageId);
    // verify(mockLogger, never()).error(anyString(), anyInt());
    // }

}