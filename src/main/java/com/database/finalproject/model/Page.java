package com.database.finalproject.model;

import java.io.DataOutputStream;
import java.io.IOException;

public interface Page {
    Row getRow(int rowId);
    int insertRow(Row row);
    boolean isFull();
    int getPid();

    void writeToBinaryFile(DataOutputStream dos) throws IOException;

    void updateBinaryFile();
}
