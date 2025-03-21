package com.database.finalproject.repository;

import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.model.BTreeImpl;
import com.database.finalproject.model.Page;
import com.database.finalproject.model.Rid;
import com.database.finalproject.model.Row;

import static com.database.finalproject.constants.PageConstants.ID_INDEX_FILE;
import static com.database.finalproject.constants.PageConstants.TITLE_INDEX_FILE;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utilities {
    public static Logger logger = LoggerFactory.getLogger(Utilities.class);

    public static void loadDataset(BufferManager bf, String filepath) {
        System.out.println("Opening file");

        BTreeImpl<String, String> idBTree = null, titleBTree = null;
        try {
            idBTree = new BTreeImpl<>(ID_INDEX_FILE, 3);
            titleBTree = new BTreeImpl<>(TITLE_INDEX_FILE, 3);
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("Index files cannot be initiated");
        }

        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            System.out.println("File opened, loading dataset to file");
            Page currPage = bf.createPage();
            int pageId = currPage.getPid();
            int currRowId = -1;

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
                currRowId++;

                Rid rid = new Rid(pageId, currRowId);
                // create bTree
                if (idBTree != null && titleBTree != null) {
                    idBTree.insert(columns[0], rid);
                    titleBTree.insert(columns[2], rid);
                }

                if (currPage.isFull()) {
                    bf.unpinPage(pageId);
                    currPage = bf.createPage();
                    pageId = currPage.getPid();
                    currRowId = -1;
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
