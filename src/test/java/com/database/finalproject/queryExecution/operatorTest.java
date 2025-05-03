package com.database.finalproject.queryExecution;

import com.database.finalproject.buffermanager.BufferManagerImpl;
import com.database.finalproject.model.Comparator;
import com.database.finalproject.model.ProjectionType;
import com.database.finalproject.model.SelectionPredicate;
import com.database.finalproject.queryplan.*;
import com.database.finalproject.model.record.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.database.finalproject.constants.PageConstants.BNL_MOVIE_WORKED_ON_INDEX;
import static com.database.finalproject.constants.PageConstants.MOVIE_ID_SIZE;
import static com.database.finalproject.constants.PageConstants.MOVIE_TITLE_SIZE;
import static com.database.finalproject.constants.PageConstants.removeTrailingBytes;
import static com.database.finalproject.constants.PageConstants.truncateOrPadByteArray;
import static org.junit.jupiter.api.Assertions.*;

class operatorTest {

    private List<MovieRecord> mockMovieRecords;
    private List<WorkedOnRecord> mockWorkedOnRecords;

    @BeforeEach
    void setUp() {
        // Create mock movie records
        mockMovieRecords = new ArrayList<>();
        byte[] movieId1 = "tt0001".getBytes(StandardCharsets.UTF_8);
        byte[] movieTitle1 = "The Matrix".getBytes(StandardCharsets.UTF_8);
        mockMovieRecords.add(new MovieRecord(movieId1, movieTitle1));

        byte[] movieId2 = "tt0002".getBytes(StandardCharsets.UTF_8);
        byte[] movieTitle2 = "The Matrix2".getBytes(StandardCharsets.UTF_8);
        mockMovieRecords.add(new MovieRecord(movieId2, movieTitle2));

        byte[] movieId3 = "tt0003".getBytes(StandardCharsets.UTF_8);
        byte[] movieTitle3 = "The Matrix3".getBytes(StandardCharsets.UTF_8);
        mockMovieRecords.add(new MovieRecord(movieId3, movieTitle3));

        // Create mock workedOn records
        mockWorkedOnRecords = new ArrayList<>();
        mockWorkedOnRecords.add(new WorkedOnRecord("tt0001".getBytes(StandardCharsets.UTF_8),
                "person1".getBytes(StandardCharsets.UTF_8),
                "director".getBytes(StandardCharsets.UTF_8)));
        mockWorkedOnRecords.add(new WorkedOnRecord("tt0002".getBytes(StandardCharsets.UTF_8),
                "person2".getBytes(StandardCharsets.UTF_8),
                "actor".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void testScanOperator() {
        MockScanOperator<MovieRecord> scan = new MockScanOperator<>(mockMovieRecords);
        scan.open();
        int count = 0;
        while (scan.next() != null)
            count++;
        scan.close();
        assertEquals(mockMovieRecords.size(), count);
    }

    @Test
    void testSelectionOperator() {
        MockScanOperator<MovieRecord> scan = new MockScanOperator<>(mockMovieRecords);
        SelectionPredicate predicate = new SelectionPredicate(1, "The Matrix",
                Comparator.GREATER_THAN_OR_EQUALS);
        SelectionOperator<MovieRecord> sel = new SelectionOperator<>(scan, List.of(predicate));
        sel.open();
        List<String> resultIds = new ArrayList<>();
        MovieRecord rec;
        while ((rec = sel.next()) != null) {
            resultIds.add(rec.getFieldByIndex(0));
        }
        sel.close();
        assertEquals(List.of("tt0001", "tt0002"), resultIds);
    }

    @Test
    void testProjectionOperator() {
        MockScanOperator<WorkedOnRecord> scan = new MockScanOperator<>(mockWorkedOnRecords);
        ProjectionOperator<WorkedOnRecord, MoviePersonRecord> proj = new ProjectionOperator<>(scan,
                ProjectionType.PROJECTION_ON_WORKED_ON);
        proj.open();

        MoviePersonRecord rec = proj.next();
        proj.close();
        assertEquals("tt0001", rec.getFieldByIndex(0));
    }

    @Test
    void testBNLJoinOperator() {
        BufferManagerImpl bufferManager = new BufferManagerImpl(10);
        MockScanOperator<MovieRecord> left = new MockScanOperator<>(mockMovieRecords);
        MockScanOperator<WorkedOnRecord> workedOn = new MockScanOperator<>(mockWorkedOnRecords);
        ProjectionOperator<WorkedOnRecord, MoviePersonRecord> proj = new ProjectionOperator<>(workedOn,
                ProjectionType.PROJECTION_ON_WORKED_ON);

        BNLJoinOperator<MovieRecord, MoviePersonRecord, MovieWorkedOnJoinRecord> join = new BNLJoinOperator<>(left,
                proj, 0, 0, 3,
                bufferManager, BNL_MOVIE_WORKED_ON_INDEX);

        join.open();
        int count = 0;
        while (join.next() != null)
            count++;
        join.close();

        assertEquals(2, count); // Only matching tt0001 and tt0002
    }

    // --- Mock Classes ---

    static class MockScanOperator<T extends ParentRecord> implements Operator<T> {
        private final List<T> records;
        private int pointer = 0;

        MockScanOperator(List<T> records) {
            this.records = records;
        }

        @Override
        public void open() {
            pointer = 0;
        }

        @Override
        public T next() {
            if (pointer >= records.size())
                return null;
            return records.get(pointer++);
        }

        @Override
        public void close() {
            pointer = 0;
        }
    }
}
