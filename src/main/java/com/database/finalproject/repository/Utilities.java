package com.database.finalproject.repository;

import com.database.finalproject.btree.BTree;
import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.model.DataPage;
import com.database.finalproject.model.Page;
import com.database.finalproject.model.Rid;
import com.database.finalproject.model.Row;

import javax.xml.crypto.Data;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import static com.database.finalproject.constants.PageConstants.DATA_PAGE_INDEX;
import static com.database.finalproject.constants.PageConstants.PADDING_BYTE;

public class Utilities {

    public static void loadDataset(BufferManager bf, String filepath) {
        System.out.println("Opening file");
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            System.out.println("File opened, loading dataset to file");
            Page currPage = bf.createPage(DATA_PAGE_INDEX);
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
                ((DataPage) currPage).insertRow(row);
                if (((DataPage) currPage).isFull()) {
                    bf.unpinPage(pageId, DATA_PAGE_INDEX);
                    currPage = bf.createPage(DATA_PAGE_INDEX);
                    pageId = currPage.getPid();
                }
            }
            bf.unpinPage(pageId, DATA_PAGE_INDEX);
            bf.force();
            System.out.println("Dataset loaded");
            System.out.println("Last Page ID: " + currPage.getPid());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createMovieIdIndex(BufferManager bf, BTree<String, Rid> b) {
        int dataPageId = 0;
        while (true) {
            Page currPage = bf.getPage(dataPageId, DATA_PAGE_INDEX);
            if (currPage == null)
                break;
            for (int i = 0; i < 105; i++) {
                Row row = ((DataPage) currPage).getRow(i);
                if (row == null)
                    break;
                b.insert(new String(row.movieId()).trim(), new Rid(dataPageId, i));
            }
            bf.unpinPage(dataPageId, DATA_PAGE_INDEX);
            dataPageId++;
        }
        bf.force();
    }

    public static void createMovieTitleIndex(BufferManager bf, BTree<String, Rid> b) {
        int dataPageId = 0;
        while (true) {
            Page currPage = bf.getPage(dataPageId, DATA_PAGE_INDEX);
            if (currPage == null)
                break;
            for (int i = 0; i < 105; i++) {
                Row row = ((DataPage) currPage).getRow(i);
                if (row == null)
                    break;
                b.insert(Arrays.toString(removeTrailingBytes(row.movieTitle())), new Rid(dataPageId, i));
            }
            bf.unpinPage(dataPageId, DATA_PAGE_INDEX);
            dataPageId++;
        }
        bf.force();
    }

    private static byte[] removeTrailingBytes(byte[] input) {
        int endIndex = input.length;
        for (int i = input.length - 1; i >= 0; i--) {
            if (input[i] != PADDING_BYTE) { // Only remove custom padding byte
                endIndex = i + 1;
                break;
            }
        }
        return Arrays.copyOf(input, endIndex);
    }
}
