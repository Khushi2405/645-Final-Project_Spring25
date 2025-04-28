package com.database.finalproject.model.page;
import com.database.finalproject.model.record.MoviePersonRecord;
import com.database.finalproject.model.record.WorkedOnRecord;

import java.util.Arrays;

import static com.database.finalproject.constants.PageConstants.*;

public class MoviePersonDataPage extends DataPage<MoviePersonRecord> {

    //    private final byte[] rows;
    private byte[] pageArray = new byte[PAGE_SIZE];
    private final int pageId;

    public MoviePersonDataPage(int pageId) {
        this.pageId = pageId;
    }

    @Override
    public byte[] getByteArray() {
        return pageArray;
    }


    public MoviePersonRecord getRecord(int rowId) {
        int totalRows = binaryToDecimal(pageArray[PAGE_SIZE - 1]);
        if (rowId >= totalRows || rowId < 0) {
//            System.out.println("WorkedOnRecord out of bounds");
            return null;
        }

        int offset = rowId * MOVIE_PERSON_ROW_SIZE;
        byte[] movieId = Arrays.copyOfRange(pageArray, offset, offset + 9);
        byte[] personId = Arrays.copyOfRange(pageArray, offset + 9, offset + 19);
        return new MoviePersonRecord(movieId, personId);
    }


    public int insertRecord(MoviePersonRecord moviePersonRecord) {
        int nextRow = binaryToDecimal(pageArray[PAGE_SIZE - 1]);
        if (nextRow == MOVIE_PERSON_ROW_LIMIT) {
            System.out.println("No space available to store more rows.");
            return -1; // Not enough space
        }
        int offset = nextRow * MOVIE_PERSON_ROW_SIZE;
        System.arraycopy(moviePersonRecord.movieId(), 0, pageArray, offset, 9);
        System.arraycopy(moviePersonRecord.personId(), 0, pageArray, offset + 9, 10);
        pageArray[PAGE_SIZE - 1] = decimalToBinary(nextRow + 1);
        return nextRow;

    }


    public boolean isFull() {
        return binaryToDecimal(pageArray[PAGE_SIZE - 1]) == MOVIE_PERSON_ROW_LIMIT;
    }

    @Override
    public int getPid() {
        return this.pageId;
    }


}

