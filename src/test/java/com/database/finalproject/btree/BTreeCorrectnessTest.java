package com.database.finalproject.btree;
import static com.database.finalproject.constants.PageConstants.INPUT_FILE;
import static com.database.finalproject.constants.PageConstants.MOVIE_TITLE_INDEX_INDEX;
import static com.database.finalproject.constants.PageConstants.PAGE_SIZE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.model.Page;
import com.database.finalproject.model.DataPage;
import com.database.finalproject.model.Rid;
import com.database.finalproject.model.Row;
import com.database.finalproject.repository.Utilities;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.test.context.TestComponent;

public class BTreeCorrectnessTest {
    @Test
    void testC1() {
        assertDoesNotThrow(() -> {
            BufferManager bf = new BufferManagerImpl(bufferSize);
            BTreeImpl movieTitleBtree = new BTreeImpl(bf, MOVIE_TITLE_INDEX_INDEX);
            Utilities.loadDataset(bf, bf.getFilePath(DATA_PAGE_INDEX));
            Utilities.createMovieTitleIndex(bf, movieTitleBtree);
        }, "Creating movie title index files failed: got an error");
    }

    @Test
    void testC2() {
        assertDoesNotThrow(() -> {
            BufferManager bf = new BufferManagerImpl(bufferSize);
            BTreeImpl movieIdBtree = new BTreeImpl(bf, MOVIE_ID_INDEX_PAGE_INDEX);
            Utilities.loadDataset(bf, bf.getFilePath(DATA_PAGE_INDEX));
            Utilities.createMovieIdIndex(bf, movieIdBtree);
        }, "Creating movieid index files failed: got an error");
    }

    @Test
    void testC2Bonus() {
        assertDoesNotThrow(() -> {
            BufferManager bf = new BufferManagerImpl(bufferSize);
            BTreeImpl movieIdBtree = new BTreeImpl(bf, MOVIE_ID_INDEX_PAGE_INDEX);
            Utilities.loadDataset(bf, bf.getFilePath(DATA_PAGE_INDEX));
            Utilities.createMovieIdIndexUsingBulkInsert(bf, movieIdBtree);
        }, "Creating movieid index files using bulk insert failed: got an error");
    }

    @Test
    void testC3MovieTitle() {
        String movieTitle = "The Derby 1895"
        BufferManager bf = new BufferManagerImpl(bufferSize);
        BTreeImpl movieTitleBtree = new BTreeImpl(bf, MOVIE_TITLE_INDEX_INDEX);
        Utilities.loadDataset(bf, bf.getFilePath(DATA_PAGE_INDEX));
        Utilities.createMovieTitleIndex(bf, movieTitleBtree);
        List<Rid> rids = movieTitleBtree.search(movieTitle);
        boolean found = false;
        for (Rid rid : rids) {
            int pageId = rid.getPageId();
            int slotId = rid.getSlotId();
            Page page = bf.getPage(pageId, 2);
            Row row = page.getRow(slotId);
            if (row.movieTitle() == movieTitle) {
                found = true;
                break;
            }
        }
        assertTrue(found, "Could not find the movie 'The Derby 1895' using the movie title Btree in the Movies table");
    }

    @Test
    void testC3MovieId() {
        String movieId = "tt0000020"
        BufferManager bf = new BufferManagerImpl(bufferSize);
        BTreeImpl movieIdBtree = new BTreeImpl(bf, MOVIE_ID_INDEX_PAGE_INDEX);
        Utilities.loadDataset(bf, bf.getFilePath(DATA_PAGE_INDEX));
        Utilities.createMovieIdIndexUsingBulkInsert(bf, movieIdBtree);
        List<Rid> rids = movieTitleBtree.search(movieId);
        boolean found = false;
        for (Rid rid : rids) {
            int pageId = rid.getPageId();
            int slotId = rid.getSlotId();
            Page page = bf.getPage(pageId, 2);
            Row row = page.getRow(slotId);
            if (row.movieId() == movieId) {
                found = true;
                break;
            }
        }
        assertTrue(found, "Could not find the movie 'tt0000020' using the movie id Btree in the Movies table");
    }
    
    void testC4MovieTitle() {
        String movieTitle1 = "Barnet Horse Fair"
        String movieTitle2 = "Blacksmith Scene"
        String movieTitleExtra = "Bataille de neige"
        BufferManager bf = new BufferManagerImpl(bufferSize);
        BTreeImpl<String,Rid> movieTitleBtree = new BTreeImpl<String,Rid>(bf, MOVIE_TITLE_INDEX_INDEX);
        Utilities.loadDataset(bf, bf.getFilePath(DATA_PAGE_INDEX));
        Utilities.createMovieTitleIndex(bf, movieTitleBtree);
        List<Rid> rids = movieTitleBtree.rangeSearch(movieTitle1, movieTitle2);
        boolean found1 = false;
        boolean found2 = false;
        boolean foundExtra = false;
        for (Rid rid : rids) {
            int pageId = rid.getPageId();
            int slotId = rid.getSlotId();
            DataPage page = (DataPage) bf.getPage(pageId, 2);
            Row row = page.getRow(slotId);
            if (row.movieTitle() == movieTitle1) {
                found1 = true;
            }
            else if 
            (row.movieTitle() == movieTitle2) {
                found2 = true;
            }
            else if (row.movieTitle() == movieTitleExtra) {
                foundExtra = true;
            }
            if (found1 && found2 && foundExtra) {
                break;
            }
        }
        assertTrue(found1 && found2 && foundExtra, "Could not find the movies using Range Search using the movie title Btree in the Movies table");
    }

    void testC4MovieId() {
        String movieId1 = "tt0000003"
        String movieId2 = "tt0000011"
        String movieIdExtra = "tt0000007"
        BufferManager bf = new BufferManagerImpl(bufferSize);
        BTreeImpl<String,Rid> movieTitleBtree = new BTreeImpl<String,Rid>(bf, MOVIE_TITLE_INDEX_INDEX);
        Utilities.loadDataset(bf, bf.getFilePath(DATA_PAGE_INDEX));
        Utilities.createMovieTitleIndex(bf, movieTitleBtree);
        List<Rid> rids = movieTitleBtree.rangeSearch(movieId1, movieId2);
        boolean found1 = false;
        boolean found2 = false;
        boolean foundExtra = false;
        for (Rid rid : rids) {
            int pageId = rid.getPageId();
            int slotId = rid.getSlotId();
            DataPage page = (DataPage) bf.getPage(pageId, 2);
            Row row = page.getRow(slotId);
            if (row.movieId() == movieId1) {
                found1 = true;
            }
            else if 
            (row.movieId() == movieId2) {
                found2 = true;
            }
            else if (row.movieId() == movieIdExtra) {
                foundExtra = true;
            }
            if (found1 && found2 && foundExtra) {
                break;
            }
        }
        assertTrue(found1 && found2 && foundExtra, "Could not find the movies using Range Search using the movie id Btree in the Movies table");
    }
}
