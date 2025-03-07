package com.database.finalproject.model;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

import static com.database.finalproject.constants.PageConstants.*;

public class PageImpl implements Page {


    private final Row[] rows;  // Stores 105 rows of 39 bytes each
    private final byte[] extraByte; // Single extra byte to ensure exact 4KB;
    public int nextRowId;
    private int pageId;

    public PageImpl(int pageId){
        //System.out.println("inside page id " + pageId);
        rows = new Row[PAGE_ROW_LIMIT]; // Allocate exactly 105 rows
        extraByte = new byte[REMAINING_BYTES];
        extraByte[0] = PADDING_BYTE;
        nextRowId = 0;
        this.pageId = pageId;
    }
    @Override
    public Row getRow(int rowId) {
        return rows[rowId];
    }

    @Override
    public int insertRow(Row row) {
        if(nextRowId < PAGE_ROW_LIMIT - 1){
            rows[nextRowId++] = row;
            return nextRowId;
        }else{
            return -1;
        }
    }

    @Override
    public boolean isFull() {
        return nextRowId == PAGE_ROW_LIMIT - 1;
    }

    @Override
    public int getPid() {
        return this.pageId;
    }


    public void writeToBinaryFile(DataOutputStream dos) throws IOException{
        int totalLen = 0;
        int index = 0;
        //System.out.println("rows length " + rows.length );
        for(Row row : rows){
            if(row == null){
                byte[] emptyArray = new byte[39];
                Arrays.fill(emptyArray, PADDING_BYTE);
                dos.write(emptyArray);
            }
            else{
                totalLen += row.getMovieId().length + row.getTitle().length;
                //System.out.println("Internal " + index++ + " " + totalLen);
                dos.write(row.getMovieId());
                dos.write(row.getTitle());
            }
        }
        dos.write(extraByte);
        //System.out.println("total " + totalLen);
    }

    @Override
    public void updateBinaryFile() {
        try (RandomAccessFile raf = new RandomAccessFile("src/main/resources/static/binary_heap.bin", "rw")){
            long offset = (long) (pageId-1) * PAGE_SIZE;
            raf.seek(offset);
            for (Row row : rows) {
                if(row != null){
                    byte[] movieId = row.getMovieId();
                    byte[] movieTitle = row.getTitle();
                    raf.write(movieId);
                    raf.write(movieTitle);
                } else {
                    // If fewer entries are provided, write empty padded entries
                    byte[] emptyArray = new byte[39];
                    Arrays.fill(emptyArray, PADDING_BYTE);
                    raf.write(emptyArray);
                }
            }
            raf.write(extraByte);
            System.out.println("Updated page " + pageId + " successfully!");

            //dos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
