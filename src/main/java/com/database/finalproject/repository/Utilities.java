package com.database.finalproject.repository;

import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.model.BTreeImpl;
import com.database.finalproject.model.BTreeNode;
import com.database.finalproject.model.Page;
import com.database.finalproject.model.Rid;
import com.database.finalproject.model.Row;

import static com.database.finalproject.constants.PageConstants.ID_INDEX_FILE;
import static com.database.finalproject.constants.PageConstants.PAGE_SIZE;
import static com.database.finalproject.constants.PageConstants.TITLE_INDEX_FILE;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utilities {
    public static Logger logger = LoggerFactory.getLogger(Utilities.class);

    public static void loadDataset(BufferManager bf, String filepath) {
        System.out.println("Opening file");

        BTreeImpl<String, String> titleBTree = null;
        try {
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
                if (titleBTree != null) {
                    // idBTree.insert(columns[0], rid);
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

            // bulk loading for id
            bulkLoadMovieId(bf);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void bulkLoadMovieId(BufferManager bf) {
        try {

            BTreeImpl<String, String> idBTree = new BTreeImpl<>(ID_INDEX_FILE, 3);
            List<BTreeNode> leafNodes = new ArrayList<>();

            int pageId = 0;
            Page page = bf.getPage(pageId);
            BTreeNode currentLeaf = new BTreeNode(true, 0);

            // read bin_heap file [all pages sequentially using bf]
            while (page != null) {
                // create leaf nodes
                byte[] pageData = page.getRows();
                int totalRows = binaryToDecimal(pageData[PAGE_SIZE - 1]);
                for (int i = 0; i < totalRows; i++) {
                    Row row = page.getRow(i);
                    if (currentLeaf.isFull()) {
                        // write leaf node to disk and store location
                        // TODO: Add node to file [via bf]
                        // mark dirty and unpin
                        leafNodes.add(currentLeaf);
                        currentLeaf = new BTreeNode<>(true, 0);
                    }
                    // insert data in node
                    currentLeaf.keys.add(row.movieId());
                    Rid rid = new Rid(pageId, i);
                    currentLeaf.children.add(rid);
                    currentLeaf.numKeys++;
                }

                // next page
                pageId++;
                page = bf.getPage(pageId);
            }
            // TODO: unpin last leaf node
            leafNodes.add(currentLeaf);

            // create non-leaf:
            List<BTreeNode> childNodes = leafNodes;
            // repeat till root
            while (childNodes.size() > 1) {
                List<BTreeNode> parentNodes = new ArrayList<>();
                BTreeNode currentParent = new BTreeNode<>(false, 0);

                for (BTreeNode child : childNodes) {
                    if (currentParent.isFull()) {
                        // TODO: markdirty and unpin
                        parentNodes.add(currentParent);
                        currentParent = new BTreeNode<>(false, 0);
                    }
                    // add first child
                    currentParent.keys.add(child.keys.get(0));
                    // TODO: get child page id
                    // currentParent.children.add()
                    currentParent.numKeys++;
                }

                // TODO: unpin last parent node
                parentNodes.add(currentParent);

                childNodes = parentNodes;
            }
            // TODO:
            // unpin childNodes[0]
            // root = childNodes[0];
            // update catalogue
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("Index files cannot be initiated");
        }
    }

    private static int binaryToDecimal(byte b) {
        return Integer.parseInt(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'), 2);
    }
}
