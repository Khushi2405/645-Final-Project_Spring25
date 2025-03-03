package com.database.finalproject.model;

public interface Page {
    Row getRow(int rowId);
    int insertRow(Row row);
    boolean isFull();
    int getPid();
}
