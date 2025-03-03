package com.database.finalproject.repository;

import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.model.Page;
import com.database.finalproject.model.PageImpl;
import com.database.finalproject.model.Row;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class Utilities {

    private static final int PAGE_SIZE = 4 * 1024; // 4 KB
    private static final int ROW_SIZE = 39; // 9 bytes for movieId + 30 bytes for title
    private static final int PAGE_ROW_LIMIT = PAGE_SIZE / ROW_SIZE;

    public static void loadDataset(BufferManager bf, String filepath){
        String outputFile = "src/main/resources/static/binary_heap.bin";
        System.out.println("Opening file");
        try (
                BufferedReader br = new BufferedReader(new FileReader(filepath));
                DataOutputStream dos = new DataOutputStream(new FileOutputStream(outputFile));
        ) {
            System.out.println("File opened with PAGE_ROW_LIMIT " + PAGE_ROW_LIMIT);
            PageImpl currPage = new PageImpl();
            // Skip header line

            String line = br.readLine();

            // Process each line from the input file
            while ((line = br.readLine()) != null) {
                String[] columns = line.split("\t");
                if (columns.length < 3) continue; // Skip invalid rows
                //System.out.println(columns[0] + " " + columns[2]);
                byte[] movieId = truncateOrPadByteArray(columns[0], 9); // tconst
                byte[] movieTitle = truncateOrPadByteArray(columns[2], 30);   // primaryTitle

                Row row = new Row(movieId, movieTitle);
                currPage.insertRow(row);
                if (currPage.isFull()) {
                    currPage.writeToBinaryFile(dos);
                    currPage = new PageImpl();
                }
            }
            currPage.writeToBinaryFile(dos);

            System.out.println("Dataset loaded");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    public static void loadDataset(String filePath) throws IOException {
//        // Load data from title.basics.tsv into buffer
//        String outputFile = "src/main/resources/static/binary_heap.bin";
//        System.out.println("Opening file");
//        try (
//                BufferedReader br = new BufferedReader(new FileReader(filePath));
//                DataOutputStream dos = new DataOutputStream(new FileOutputStream(outputFile));
//        ) {
//            System.out.println("File opened with PAGE_ROW_LIMIT " + PAGE_ROW_LIMIT);
//            PageImpl currPage = new PageImpl(PAGE_ROW_LIMIT);
//            // Skip header line
//
//            String line = br.readLine();
//
//            // Process each line from the input file
//            while ((line = br.readLine()) != null) {
//                String[] columns = line.split("\t");
//                if (columns.length < 3) continue; // Skip invalid rows
//                //System.out.println(columns[0] + " " + columns[2]);
//                byte[] movieId = truncateOrPadByteArray(columns[0], 9); // tconst
//                byte[] movieTitle = truncateOrPadByteArray(columns[2], 30);   // primaryTitle
//
//                Row row = new Row(movieId, movieTitle);
//                currPage.insertRow(row);
//                if (currPage.isFull()) {
//                    currPage.writeToBinaryFile(dos);
//                    PageImpl.totalPages++;
//                    System.out.println(PageImpl.totalPages + "  written");
//                    currPage = new PageImpl(PAGE_ROW_LIMIT);
//                }
//            }
//            if (currPage.getTotalRows() > 0) {
//                currPage.writeToBinaryFile(dos);
//                PageImpl.totalPages++;
//                System.out.println(PageImpl.totalPages + "  written");
//            }
//
//            System.out.println("Dataset loaded");
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public static void fetchPage(int pageId) throws IOException{
//        String inputFile = "src/main/resources/static/binary_heap.bin";
//        try(RandomAccessFile raf = new RandomAccessFile(inputFile, "r")){
//            long offset = pageId * PAGE_ROW_LIMIT * 39;
//            raf.seek(offset);
//            PageImpl page = new PageImpl(PAGE_ROW_LIMIT);
//            for(int i = 0 ; i < PAGE_ROW_LIMIT; i++){
//                if(raf.getFilePointer() < raf.length()){
//                    byte[] moveId = new byte[9];
//                    byte[] movieTitle = new byte[30];
//                    raf.read(moveId);
//                    raf.read(movieTitle);
//
////                    Row row = new Row(moveId, movieTitle);
//                    System.out.println(new String(moveId).trim());
//                    System.out.println(new String(movieTitle).trim());
//
//                }
//            }
//        }
//    }
//
    private static byte[] truncateOrPadByteArray(String value, int maxLength) {
        if (value.length() > maxLength) {
            value = value.substring(0, maxLength);
        } else {
            while (value.length() < maxLength) {
                value += "\0";
            }
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
