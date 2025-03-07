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

    public static void loadDataset(BufferManager bf, String filepath){
        String outputFile = "src/main/resources/static/binary_heap.bin";
        System.out.println("Opening file");
        try (
                BufferedReader br = new BufferedReader(new FileReader(filepath));
                DataOutputStream dos = new DataOutputStream(new FileOutputStream(outputFile));
        ) {
            System.out.println("File opened with PAGE_ROW_LIMIT " + PAGE_ROW_LIMIT);
            Page currPage = bf.createPageToLoadDataset();
            // Skip header line

            String line = br.readLine();

            // Process each line from the input file
            while ((line = br.readLine()) != null) {
                String[] columns = line.split("\t");
                if (columns.length < 3) continue; // Skip invalid rows
                //System.out.println(columns[0] + " " + columns[2]);
                byte[] movieId =  columns[0].getBytes(StandardCharsets.UTF_8); // tconst
                byte[] movieTitle = columns[2].getBytes(StandardCharsets.UTF_8);   // primaryTitle

                Row row = new Row(movieId, movieTitle);
                currPage.insertRow(row);
                if (currPage.isFull()) {
                    currPage.writeToBinaryFile(dos);
                    //System.out.println(currPage.getPid());
                    currPage = bf.createPageToLoadDataset();
                }
            }
            currPage.writeToBinaryFile(dos);

            System.out.println("Dataset loaded");
            System.out.println(currPage.getPid());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void fetchPage(int pageId) throws IOException{
        String inputFile = "src/main/resources/static/binary_heap.bin";
        try(RandomAccessFile raf = new RandomAccessFile(inputFile, "r")){
            long offset = (long) (pageId-1) * PAGE_SIZE;
            raf.seek(offset);
            //PageImpl page = new PageImpl(PAGE_ROW_LIMIT);
            for(int i = 0 ; i < PAGE_ROW_LIMIT; i++){
                System.out.println(i);
                if(raf.getFilePointer() < raf.length()){
                    byte[] moveId = new byte[9];
                    byte[] movieTitle = new byte[30];
                    raf.read(moveId);
                    raf.read(movieTitle);
                    moveId = removeTrailingBytes(moveId);
                    movieTitle = removeTrailingBytes(movieTitle);
                    //Row row = new Row(moveId, movieTitle);
                    System.out.println(new String(moveId).trim());
                    System.out.println(new String(movieTitle).trim());

                }
            }
        }
    }
//


    private static byte[] removeTrailingBytes(byte[] input) {
        int endIndex = input.length;
        for (int i = input.length - 1; i >= 0; i--) {
            if (input[i] != PADDING_BYTE) {  // Only remove our custom padding byte
                endIndex = i + 1;
                break;
            }
        }
        return Arrays.copyOf(input, endIndex);
    }
}
