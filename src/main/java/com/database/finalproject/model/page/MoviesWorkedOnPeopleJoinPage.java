package com.database.finalproject.model.page;

import com.database.finalproject.model.record.MovieWorkedOnJoinRecord;
import com.database.finalproject.model.record.MovieWorkedOnPeopleJoinRecord;
import com.database.finalproject.model.record.WorkedOnRecord;

import java.util.Arrays;

import static com.database.finalproject.constants.PageConstants.*;

// 30 records per page of size 135
public class MoviesWorkedOnPeopleJoinPage extends DataPage<MovieWorkedOnPeopleJoinRecord> {

    // private final byte[] rows;
    private byte[] pageArray = new byte[PAGE_SIZE];
    private final int pageId;

    public MoviesWorkedOnPeopleJoinPage(int pageId) {
        this.pageId = pageId;
    }

    @Override
    public byte[] getByteArray() {
        return pageArray;
    }

    public MovieWorkedOnPeopleJoinRecord getRecord(int rowId) {
        int totalRows = binaryToDecimal(pageArray[PAGE_SIZE - 1]);
        if (rowId >= totalRows || rowId < 0) {
            // System.out.println("MoviesWorkedOnPeople row out of bounds");
            return null;
        }

        int offset = rowId * MOVIE_WORKED_ON_PEOPLE_JOIN_ROW_SIZE;
        byte[] title = Arrays.copyOfRange(pageArray, offset, offset + 30); // 30 bytes for title
        byte[] name = Arrays.copyOfRange(pageArray, offset + 30, offset + 135); // 105 bytes for name
        return new MovieWorkedOnPeopleJoinRecord(title, name);
    }

    public int insertRecord(MovieWorkedOnPeopleJoinRecord movieWorkedOnPeopleRecord) {
        int nextRow = binaryToDecimal(pageArray[PAGE_SIZE - 1]);
        // TODO: -1 or not?
        if (nextRow == MOVIE_WORKED_ON_PEOPLE_JOIN_ROW_LIMIT) {
            System.out.println("No space available to store more rows.");
            return -1; // Not enough space
        }
        int offset = nextRow * MOVIE_WORKED_ON_PEOPLE_JOIN_ROW_SIZE;
        System.arraycopy(movieWorkedOnPeopleRecord.title(), 0, pageArray, offset, 30);
        System.arraycopy(movieWorkedOnPeopleRecord.name(), 0, pageArray, offset + 30, 105);
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
