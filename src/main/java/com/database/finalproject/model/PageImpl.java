package com.database.finalproject.model;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PageImpl implements Page {
    Map<Integer,Row> rows;
    int maxRows;
    int nextRowId;
    public static int totalPages = 0;

    public PageImpl(int maxRows){
        rows = new HashMap<>();
        this.maxRows = maxRows;
        this.nextRowId = -1;
    }
    @Override
    public Row getRow(int rowId) {
        return rows.get(rowId);
    }

    @Override
    public int insertRow(Row row) {
        if(rows.size() < maxRows){
            rows.put(++nextRowId,row);
            return nextRowId;
        }else{
            return -1;
        }
    }

    @Override
    public boolean isFull() {
        return rows.size() >= maxRows;
    }

    public int getTotalRows(){
        return rows.size();
    }

    public void writeToBinaryFile(DataOutputStream dos) throws IOException{
        for(Row row : rows.values()){
            dos.write(row.getMovieId());
            dos.write(row.getTitle());
        }
    }

}
