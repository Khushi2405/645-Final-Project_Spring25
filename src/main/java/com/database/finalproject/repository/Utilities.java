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
        try (BufferedReader br = new BufferedReader(new FileReader(filepath));) {
            System.out.println("File opened with PAGE_ROW_LIMIT " + PAGE_ROW_LIMIT);
            Page currPage = bf.createPage();
            int pageId = currPage.getPid();
            // bf.markDirty(pageId);

            // Skip header line
            String line = br.readLine();

            // Process each line from the input file
            while ((line = br.readLine()) != null) {
                String[] columns = line.split("\t");
                if (columns.length < 3)
                    continue; // Skip invalid rows
                byte[] movieId = columns[0].getBytes(); // tconst
                byte[] movieTitle = columns[2].getBytes(); // primaryTitle

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
            System.out.println(currPage.getPid());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
