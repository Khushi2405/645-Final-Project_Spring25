package com.database.finalproject.btree;

import static com.database.finalproject.constants.PageConstants.MOVIE_ID_INDEX_PAGE_INDEX;
import static com.database.finalproject.constants.PageConstants.MOVIE_TITLE_INDEX_INDEX;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.buffermanager.BufferManagerImpl;
import com.database.finalproject.controller.UserController;
import com.database.finalproject.model.Rid;
import com.database.finalproject.model.Row;
import com.database.finalproject.repository.Utilities;

public class btreeCorrectnessTest {
    // private static BufferManager bf;
    // private static BTree<String, Rid> movieTitleIndex;
    // private static BTree<String, Rid> movieIdIndex;
    private static UserController uc;

    @BeforeAll
    static void setUp() {
        uc = new UserController(5);
        // bf = new BufferManagerImpl(5); // Initialize BufferManager
        // movieIdIndex = new BTreeImpl(bf, MOVIE_ID_INDEX_PAGE_INDEX); // Initialize B+
        // Tree for titles
        // movieTitleIndex = new BTreeImpl(bf, MOVIE_TITLE_INDEX_INDEX); // Initialize
        // B+ Tree for movie IDs

        // Build indexes once for all tests
        // Utilities.createMovieTitleIndex(bf, movieTitleIndex);
        // Utilities.createMovieIdIndex(bf, movieIdIndex);
    }

    @Test
    void testC1_VerifyMovieTitleIndex_SearchSingleKey() {
        // Test case for "The Boxing Kangaroo"
        List<Row> titleList = uc.searchMovieTitle("The Boxing Kangaroo");
        System.out.println("Result is - " + titleList);

        assertFalse(titleList.isEmpty(), "Index should contain 'The Boxing Kangaroo'");
        assertTrue(titleList.stream().anyMatch(row -> "The Boxing Kangaroo".equals(row.movieTitle())),
                "Expected movie title 'The Boxing Kangaroo' in the index");

        // Test case for "A Hard Wash"
        titleList = uc.searchMovieTitle("A Hard Wash");
        System.out.println(titleList);

        assertFalse(titleList.isEmpty(), "Index should contain 'A Hard Wash'");
        assertTrue(titleList.stream().anyMatch(row -> "A Hard Wash".equals(row.movieTitle())),
                "Expected movie title 'A Hard Wash' in the index");
    }

    @Test
    void testC2_VerifyMovieIdIndex_SearchSingleKey() {
        // Test case for movieId 'tt0000048'
        List<Row> idList = uc.searchMovieId("tt0000048"); // The Boxing Kangaroo
        System.out.println(idList);
        assertFalse(idList.isEmpty(), "Index should contain movieId 'tt0000048'");
        assertTrue(idList.stream().anyMatch(row -> "tt0000048".equals(row.movieId())),
                "Expected movieId 'tt0000048' in the index");

        // Test case for movieId 'tt0000082'
        idList = uc.searchMovieId("tt0000082"); // A Hard Wash
        System.out.println(idList);

        assertFalse(idList.isEmpty(), "Index should contain movieId 'tt0000082'");
        assertTrue(idList.stream().anyMatch(row -> "tt0000082".equals(row.movieId())),
                "Expected movieId 'tt0000082' in the index");
    }

    // @Test
    // void testC3_SearchSingleKey() {
    // Iterator<Rid> titleItr = movieTitleIndex.search("A Terrible Night");
    // assertTrue(titleItr.hasNext(), "Search should find 'A Terrible Night' in
    // title index");

    // Iterator<Rid> idItr = movieIdIndex.search("tt0000131"); // A Terrible Night
    // assertTrue(idItr.hasNext(), "Search should find movieId 'tt0000131' in
    // movieId index");
    // }

    @Test
    void testC4_SearchRangeOfKeys() {
        // Test for range search by title
        List<Row> titleResults = uc.rangeSearchMovieTitle("A Hard Wash", "A Terrible Night");
        System.out.println(titleResults);

        assertFalse(titleResults.isEmpty(), "Range search should return titles from A Hard Wash to A Terrible Night");

        // Test for range search by movie ID
        List<Row> idResults = uc.rangeSearchMovieId("tt0000082", "tt0000131");
        System.out.println(idResults);

        assertFalse(idResults.isEmpty(), "Range search should return movie IDs in range");
    }
}
