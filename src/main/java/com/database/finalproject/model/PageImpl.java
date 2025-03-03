package com.database.finalproject.model;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PageImpl implements Page {
    private static final int PAGE_SIZE = 4096;  // 4KB page size
    private static final int ROW_SIZE = 39;    // Each row is 39 bytes
    private static final int TOTAL_ROWS = PAGE_SIZE / ROW_SIZE; // 105 rows
    private static final int REMAINING_BYTES = PAGE_SIZE % ROW_SIZE; // 1 byte

    private final Row[] rows;  // Stores 105 rows of 39 bytes each
    private final byte extraByte; // Single extra byte to ensure exact 4KB;
    private int nextRowId;
    private int pageId;

    public PageImpl(int pageId){
        rows = new Row[TOTAL_ROWS]; // Allocate exactly 105 rows
        extraByte = REMAINING_BYTES;
        nextRowId = -1;
        pageId = pageId;
    }
    @Override
    public Row getRow(int rowId) {
        return rows[rowId];
    }

    @Override
    public int insertRow(Row row) {
        if(nextRowId < TOTAL_ROWS){
            rows[++nextRowId] = row;
            return nextRowId;
        }else{
            return -1;
        }
    }

    @Override
    public boolean isFull() {
        return nextRowId == TOTAL_ROWS;
    }

    @Override
    public int getPid() {
        return this.pageId;
    }


    public void writeToBinaryFile(DataOutputStream dos) throws IOException{
        for(Row row : rows){
            dos.write(row.getMovieId());
            dos.write(row.getTitle());
        }
    }

}
