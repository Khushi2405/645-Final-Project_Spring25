package com.database.finalproject.model;

import static com.database.finalproject.constants.PageConstants.PAGE_SIZE;

public interface Page {
//    Row getRow(int rowId);
//    int insertRow(Row row);
//    boolean isFull();
    byte[] pageArray = new byte[PAGE_SIZE];

    int getPid();
    byte[] getByteArray();


}
