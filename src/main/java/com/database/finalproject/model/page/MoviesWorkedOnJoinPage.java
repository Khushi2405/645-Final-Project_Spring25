package com.database.finalproject.model.page;

import com.database.finalproject.model.record.MovieWorkedOnJoinRecord;
import com.database.finalproject.model.record.WorkedOnRecord;

import java.util.Arrays;

import static com.database.finalproject.constants.PageConstants.*;

// 83 records per page of size 49-bytes each
public class MoviesWorkedOnJoinPage extends DataPage<MovieWorkedOnJoinRecord> {

    // private final byte[] rows;
    private byte[] pageArray = new byte[PAGE_SIZE];
    private final int pageId;

    public MoviesWorkedOnJoinPage(int pageId) {
        this.pageId = pageId;
    }

    @Override
    public byte[] getByteArray() {
        return pageArray;
    }

    public MovieWorkedOnJoinRecord getRecord(int rowId) {
        int totalRows = binaryToDecimal(pageArray[PAGE_SIZE - 1]);
        if (rowId >= totalRows || rowId < 0) {
            // System.out.println("WorkedOnRecord out of bounds");
            return null;
        }

        int offset = rowId * MOVIE_WORKED_ON_JOIN_ROW_SIZE;
        byte[] movieId = Arrays.copyOfRange(pageArray, offset, offset + 9); // 9 bytes for movieId
        byte[] personId = Arrays.copyOfRange(pageArray, offset + 9, offset + 19); // 10 bytes for personID
        byte[] title = Arrays.copyOfRange(pageArray, offset + 19, offset + 49); // 30 bytes for title
        return new MovieWorkedOnJoinRecord(movieId, personId, title);
    }

    public int insertRecord(MovieWorkedOnJoinRecord movieWorkedOnRecord) {
        int nextRow = binaryToDecimal(pageArray[PAGE_SIZE - 1]);
        // TODO: -1 or not?
        if (nextRow == MOVIE_WORKED_ON_JOIN_ROW_LIMIT) {
            System.out.println("No space available to store more rows.");
            return -1; // Not enough space
        }
        int offset = nextRow * MOVIE_WORKED_ON_JOIN_ROW_SIZE;
        System.arraycopy(movieWorkedOnRecord.movieId(), 0, pageArray, offset, 9);
        System.arraycopy(movieWorkedOnRecord.personId(), 0, pageArray, offset + 9, 10);
        System.arraycopy(movieWorkedOnRecord.title(), 0, pageArray, offset + 19, 30);
        pageArray[PAGE_SIZE - 1] = decimalToBinary(nextRow + 1);
        return nextRow;

    }

    public boolean isFull() {
        return binaryToDecimal(pageArray[PAGE_SIZE - 1]) == MOVIE_WORKED_ON_JOIN_ROW_LIMIT;
    }

    @Override
    public int getPid() {
        return this.pageId;
    }

}
