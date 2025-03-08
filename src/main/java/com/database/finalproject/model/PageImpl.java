package com.database.finalproject.model;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

import static com.database.finalproject.constants.PageConstants.*;

public class PageImpl implements Page {

    private final byte[] rows;
    private final int pageId;

    public PageImpl(int pageId){
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
        int nextRow = binaryToDecimal(rows[PAGE_SIZE-1]);
        int offset = nextRow*39;
        if (offset == 4095) {
            System.out.println("No space available to store more rows.");
            return -1; // Not enough space
        }
        System.arraycopy(row.movieId(), 0, rows, offset, 9);
        System.arraycopy(row.movieTitle(), 0, rows, offset + 9, 30);
        rows[PAGE_SIZE-1] = decimalToBinary(nextRow+1);
        return nextRow;

    }

    @Override
    public boolean isFull() {
        return binaryToDecimal(rows[PAGE_SIZE-1]) == PAGE_ROW_LIMIT;
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


//    public void writeToBinaryFile(DataOutputStream dos) throws IOException{
//        int totalLen = 0;
//        int index = 0;
//        //System.out.println("rows length " + rows.length );
//        for(Row row : rows){
//            if(row == null){
//                byte[] emptyArray = new byte[39];
//                Arrays.fill(emptyArray, PADDING_BYTE);
//                dos.write(emptyArray);
//            }
//            else{
//                totalLen += row.movieId().length + row.title().length;
//                //System.out.println("Internal " + index++ + " " + totalLen);
//                dos.write(row.movieId());
//                dos.write(row.title());
//            }
//        }
//        dos.write(extraByte);
//        //System.out.println("total " + totalLen);
//    }
//
//    @Override
//    public void updateBinaryFile() {
//        try (RandomAccessFile raf = new RandomAccessFile("src/main/resources/static/binary_heap.bin", "rw")){
//            long offset = (long) (pageId-1) * PAGE_SIZE;
//            raf.seek(offset);
//            for (Row row : rows) {
//                if(row != null){
//                    byte[] movieId = row.movieId();
//                    byte[] movieTitle = row.title();
//                    raf.write(movieId);
//                    raf.write(movieTitle);
//                } else {
//                    // If fewer entries are provided, write empty padded entries
//                    byte[] emptyArray = new byte[39];
//                    Arrays.fill(emptyArray, PADDING_BYTE);
//                    raf.write(emptyArray);
//                }
//            }
//            raf.write(extraByte);
//            System.out.println("Updated page " + pageId + " successfully!");
//
//            //dos.flush();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }

}
