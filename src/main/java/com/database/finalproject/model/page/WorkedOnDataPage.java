package com.database.finalproject.model.page;
import com.database.finalproject.model.record.WorkedOnRecord;

import java.util.Arrays;

import static com.database.finalproject.constants.PageConstants.*;

public class WorkedOnDataPage extends DataPage<WorkedOnRecord> {

    //    private final byte[] rows;
    private byte[] pageArray = new byte[PAGE_SIZE];
    private final int pageId;

    public WorkedOnDataPage(int pageId) {
        this.pageId = pageId;
    }

    @Override
    public byte[] getByteArray() {
        return pageArray;
    }


    public WorkedOnRecord getRecord(int rowId) {
        int totalRows = binaryToDecimal(pageArray[PAGE_SIZE - 1]);
        if (rowId >= totalRows || rowId < 0) {
//            System.out.println("WorkedOnRecord out of bounds");
            return null;
        }

        int offset = rowId * WORKED_ON_ROW_SIZE;
        byte[] movieId = Arrays.copyOfRange(pageArray, offset, offset + 9);
        byte[] personId = Arrays.copyOfRange(pageArray, offset + 9, offset + 19);
        byte[] category = Arrays.copyOfRange(pageArray, offset + 19, offset + 39);
        return new WorkedOnRecord(movieId, personId, category);
    }


    public int insertRecord(WorkedOnRecord workedOnRecord) {
        int nextRow = binaryToDecimal(pageArray[PAGE_SIZE - 1]);
        int offset = nextRow * WORKED_ON_ROW_SIZE;
        if (offset == 4025) {
            System.out.println("No space available to store more rows.");
            return -1; // Not enough space
        }
        System.arraycopy(workedOnRecord.movieId(), 0, pageArray, offset, 9);
        System.arraycopy(workedOnRecord.personId(), 0, pageArray, offset + 9, 10);
        System.arraycopy(workedOnRecord.category(), 0, pageArray, offset + 19, 20);
        pageArray[PAGE_SIZE - 1] = decimalToBinary(nextRow + 1);
        return nextRow;

    }


    public boolean isFull() {
        return binaryToDecimal(pageArray[PAGE_SIZE - 1]) == WORKED_ON_PAGE_ROW_LIMIT;
    }

    @Override
    public int getPid() {
        return this.pageId;
    }


}

