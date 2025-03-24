package com.database.finalproject.repository;

import com.database.finalproject.btree.BTree;
import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.model.Page;
import com.database.finalproject.model.Rid;
import com.database.finalproject.model.Row;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Utilities {

    public static void loadDataset(BufferManager bf, String filepath) {
        System.out.println("Opening file");
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            System.out.println("File opened, loading dataset to file");
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
                    currPage = bf.createPage();
                    pageId = currPage.getPid();
                }
            }
            bf.unpinPage(pageId);
            System.out.println("Dataset loaded");
            System.out.println("Last Page ID: " + currPage.getPid());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createMovieIdIndex(BufferManager bf, BTree<String, Rid> b){

    }
}

