package com.database.finalproject.model.page;
import com.database.finalproject.model.record.PeopleRecord;

import java.util.Arrays;

import static com.database.finalproject.constants.PageConstants.*;

public class PeopleDataPage extends DataPage<PeopleRecord> {

    //    private final byte[] rows;
    private byte[] pageArray = new byte[PAGE_SIZE];
    private final int pageId;

    public PeopleDataPage(int pageId) {
        this.pageId = pageId;
    }

    @Override
    public byte[] getByteArray() {
        return pageArray;
    }


    public PeopleRecord getRecord(int rowId) {
        int totalRows = binaryToDecimal(pageArray[PAGE_SIZE - 1]);
        if (rowId >= totalRows || rowId < 0) {
//            System.out.println("PeopleRecord out of bounds");
            return null;
        }

        int offset = rowId * PEOPLE_ROW_SIZE;
        byte[] personId = Arrays.copyOfRange(pageArray, offset, offset + 10);
        byte[] name = Arrays.copyOfRange(pageArray, offset + 10, offset + 115);
        return new PeopleRecord(personId, name);
    }


    public int insertRecord(PeopleRecord peopleRecord) {
        int nextRow = binaryToDecimal(pageArray[PAGE_SIZE - 1]);
        if (nextRow == PEOPLE_PAGE_ROW_LIMIT) {
            System.out.println("No space available to store more rows.");
            return -1; // Not enough space
        }
        int offset = nextRow * PEOPLE_ROW_SIZE;
        System.arraycopy(peopleRecord.personId(), 0, pageArray, offset, 10);
        System.arraycopy(peopleRecord.name(), 0, pageArray, offset + 10, 105);
        pageArray[PAGE_SIZE - 1] = decimalToBinary(nextRow + 1);
        return nextRow;

    }


    public boolean isFull() {
        return binaryToDecimal(pageArray[PAGE_SIZE - 1]) == PEOPLE_PAGE_ROW_LIMIT;
    }

    @Override
    public int getPid() {
        return this.pageId;
    }


}

