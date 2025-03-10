package com.database.finalproject.model;

import static com.database.finalproject.constants.PageConstants.PAGE_ROW_LIMIT;
import static com.database.finalproject.constants.PageConstants.PAGE_SIZE;

import java.util.Arrays;

public class PageImpl implements Page {

    private final byte[] rows;
    private final int pageId;

    public PageImpl(int pageId) {
        rows = new byte[PAGE_SIZE];
        this.pageId = pageId;
    }

    @Override
    public byte[] getRows() {
        return rows;
    }

    @Override
    public Row getRow(int rowId) {
        int totalRows = binaryToDecimal(rows[PAGE_SIZE - 1]);
        if (rowId >= totalRows || rowId < 0) {
            System.out.println("Row out of bounds");
            return null;
        }

        int offset = rowId * 39;
        byte[] movieId = Arrays.copyOfRange(rows, offset, offset + 9);
        byte[] movieTitle = Arrays.copyOfRange(rows, offset + 9, offset + 39);
        return new Row(movieId, movieTitle);
    }

    @Override
    public int insertRow(Row row) {
        int nextRow = binaryToDecimal(rows[PAGE_SIZE - 1]);
        int offset = nextRow * 39;
        if (offset == 4095) {
            System.out.println("No space available to store more rows.");
            return -1; // Not enough space
        }
        System.arraycopy(row.movieId(), 0, rows, offset, 9);
        System.arraycopy(row.movieTitle(), 0, rows, offset + 9, 30);
        rows[PAGE_SIZE - 1] = decimalToBinary(nextRow + 1);
        return nextRow;

    }

    @Override
    public boolean isFull() {
        return binaryToDecimal(rows[PAGE_SIZE - 1]) == PAGE_ROW_LIMIT;
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

