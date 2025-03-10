package com.database.finalproject.repository;

import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.model.Page;
import com.database.finalproject.model.PageImpl;
import com.database.finalproject.model.Row;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.database.finalproject.constants.PageConstants.*;

public class Utilities {

    public static void loadDataset(BufferManager bf, String filepath) {
        System.out.println("Opening file");
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            System.out.println("File opened with PAGE_ROW_LIMIT " + PAGE_ROW_LIMIT);
            Page currPage = bf.createPage();
            int pageId = currPage.getPid();

            // Skip header line
            String line = br.readLine();

            // Process each line from the input file
            while ((line = br.readLine()) != null) {
                String[] columns = line.split("\t");
                if (columns.length < 3)
                    continue; // Skip invalid rows
                byte[] movieId = columns[0].getBytes(); // tconst
                byte[] movieTitle = columns[2].getBytes(); // primaryTitle
                if (movieId.length > 9) {
                    continue;
                }

                Row row = new Row(movieId, movieTitle);
                currPage.insertRow(row);
                if (currPage.isFull()) {
                    bf.unpinPage(pageId);
                    // TODO - check if next line is not null
                    currPage = bf.createPage();
                    pageId = currPage.getPid();
                    // bf.markDirty(pageId);
                }
            }
            bf.unpinPage(pageId);
            System.out.println("Dataset loaded");
            System.out.println("Last Page ID: " + currPage.getPid());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

// public static void fetchPage(int pageId) throws IOException{
// String inputFile = "src/main/resources/static/binary_heap.bin";
// try(RandomAccessFile raf = new RandomAccessFile(inputFile, "r")){
// long offset = (long) (pageId-1) * PAGE_SIZE;
// raf.seek(offset);
// //PageImpl page = new PageImpl(PAGE_ROW_LIMIT);
// for(int i = 0 ; i < PAGE_ROW_LIMIT; i++){
// System.out.println(i);
// if(raf.getFilePointer() < raf.length()){
// byte[] moveId = new byte[9];
// byte[] movieTitle = new byte[30];
// raf.read(moveId);
// raf.read(movieTitle);
// moveId = removeTrailingBytes(moveId);
// movieTitle = removeTrailingBytes(movieTitle);
// //Row row = new Row(moveId, movieTitle);
// System.out.println(new String(moveId).trim());
// System.out.println(new String(movieTitle).trim());
//
// }
// }
// }
// }
//

// private static byte[] removeTrailingBytes(byte[] input) {
// int endIndex = input.length;
// for (int i = input.length - 1; i >= 0; i--) {
// if (input[i] != PADDING_BYTE) { // Only remove our custom padding byte
// endIndex = i + 1;
// break;
// }
// }
// return Arrays.copyOf(input, endIndex);
// }
