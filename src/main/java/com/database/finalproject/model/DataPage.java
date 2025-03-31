package com.database.finalproject.model;

import static com.database.finalproject.constants.PageConstants.PAGE_ROW_LIMIT;
import static com.database.finalproject.constants.PageConstants.PAGE_SIZE;

import java.util.Arrays;

public class DataPage implements Page {

//    private final byte[] rows;
    private byte[] pageArray = new byte[PAGE_SIZE];
    private final int pageId;

    public DataPage(int pageId) {
        this.pageId = pageId;
    }

    @Override
    public byte[] getByteArray() {
        return pageArray;
    }


    public Row getRow(int rowId) {
        int totalRows = binaryToDecimal(pageArray[PAGE_SIZE - 1]);
        if (rowId >= totalRows || rowId < 0) {
            System.out.println("Row out of bounds");
            return null;
        }

        int offset = rowId * 39;
        byte[] movieId = Arrays.copyOfRange(pageArray, offset, offset + 9);
        byte[] movieTitle = Arrays.copyOfRange(pageArray, offset + 9, offset + 39);
        return new Row(movieId, movieTitle);
    }


    public int insertRow(Row row) {
        int nextRow = binaryToDecimal(pageArray[PAGE_SIZE - 1]);
        int offset = nextRow * 39;
        if (offset == 4095) {
            System.out.println("No space available to store more rows.");
            return -1; // Not enough space
        }
        System.arraycopy(row.movieId(), 0, pageArray, offset, 9);
        System.arraycopy(row.movieTitle(), 0, pageArray, offset + 9, 30);
        pageArray[PAGE_SIZE - 1] = decimalToBinary(nextRow + 1);
        return nextRow;

    }


    public boolean isFull() {
        return binaryToDecimal(pageArray[PAGE_SIZE - 1]) == PAGE_ROW_LIMIT;
    }

    @Override
    public int getPid() {
        return this.pageId;
    }

    // Convert a binary-stored byte to decimal
    private static int binaryToDecimal(byte b) {
        return Integer.parseInt(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'), 2);
    }

    // Convert a decimal value to binary format in a single byte
    private static byte decimalToBinary(int num) {
        return (byte) Integer.parseInt(Integer.toBinaryString(num), 2);
    }
}

