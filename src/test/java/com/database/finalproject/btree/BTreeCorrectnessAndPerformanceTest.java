package com.database.finalproject.btree;

import static com.database.finalproject.constants.PageConstants.DATA_PAGE_INDEX;
import static com.database.finalproject.constants.PageConstants.MOVIE_ID_INDEX_PAGE_INDEX;
import static com.database.finalproject.constants.PageConstants.MOVIE_TITLE_INDEX_INDEX;
import static com.database.finalproject.constants.PageConstants.PAGE_SIZE;
import static com.database.finalproject.constants.PageConstants.removeTrailingBytes;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import javax.swing.*;
import java.awt.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.buffermanager.BufferManagerImpl;
import com.database.finalproject.controller.UserController;
import com.database.finalproject.model.Page;
import com.database.finalproject.model.DataPage;
import com.database.finalproject.model.Rid;
import com.database.finalproject.model.Row;
import com.database.finalproject.repository.Utilities;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.test.context.TestComponent;

public class BTreeCorrectnessAndPerformanceTest {
    private static BufferManager bf;
    private static BTreeImpl movieIdBtree;
    private static BTreeImpl movieTitleBtree;
    // private UserController controller;

    @BeforeAll
    static void setup() {
        // To create index files/binary file of dataset, uncomment lines 28, 29, 30 in
        // UserController.java
        int bufferSize = 5;
        bf = new BufferManagerImpl(bufferSize);
        movieIdBtree = new BTreeImpl(bf, MOVIE_ID_INDEX_PAGE_INDEX);
        movieTitleBtree = new BTreeImpl(bf, MOVIE_TITLE_INDEX_INDEX);
        // controller = new UserController(bufferSize);
    }

    @AfterEach
    void cleanup() {
        bf.force();
    }

    @Test
    void testC1() {
        /*
         * Uncomment creating movie title index file from setup
         */
        assertDoesNotThrow(() -> {
        }, "Creating movie title index files failed: got an error");
    }

    @Test
    void testC2() {
        /*
         * Uncomment creating movie id index file from setup
         */
        assertDoesNotThrow(() -> {
        }, "Creating movieid index files failed: got an error");
    }

    @Test
    void testC2Bonus() {
        /*
         * Uncomment creating movie id index file using bulk insert from setup
         */
        assertDoesNotThrow(() -> {
        }, "Creating movieid index files using bulk insert failed: got an error");
    }

    @Test
    void testC3MovieTitle() {
        String movieTitle = "The Derby 1895";
        Iterator<Rid> rids = movieTitleBtree.search(movieTitle);
        boolean found = false;
        while (rids.hasNext()) {
            Rid rid = rids.next();
            int pageId = rid.getPageId();
            int slotId = rid.getSlotId();
            DataPage page = (DataPage) bf.getPage(pageId, DATA_PAGE_INDEX);
            Row row = page.getRow(slotId);
            String rowTitle = new String(removeTrailingBytes(row.movieTitle()), StandardCharsets.UTF_8).trim();
            if (rowTitle.equals(movieTitle)) {
                found = true;
                break;
            }
            bf.unpinPage(pageId, DATA_PAGE_INDEX);
        }
        assertTrue(found, "Could not find the movie 'The Derby 1895' using the movie title Btree in the Movies table");
    }

    @Test
    void testC3MovieId() {
        String movieId = "tt0000020";
        Iterator<Rid> rids = movieIdBtree.search(movieId);
        boolean found = false;
        while (rids.hasNext()) {
            Rid rid = rids.next();
            int pageId = rid.getPageId();
            int slotId = rid.getSlotId();
            DataPage page = (DataPage) bf.getPage(pageId, DATA_PAGE_INDEX);
            Row row = page.getRow(slotId);
            String rowId = new String(removeTrailingBytes(row.movieId()), StandardCharsets.UTF_8).trim();
            if (rowId.equals(movieId)) {
                found = true;
                break;
            }
            bf.unpinPage(pageId, DATA_PAGE_INDEX);
        }
        assertTrue(found, "Could not find the movie 'tt0000020' using the movie id Btree in the Movies table");
    }

    @Test
    void testC4MovieTitle() {
        String movieTitle1 = "Barnet Horse Fair";
        String movieTitle2 = "Blacksmith Scene";
        String movieTitleExtra = "Bataille de neige";
        Iterator<Rid> rids = movieTitleBtree.rangeSearch(movieTitle1, movieTitle2);
        boolean found1 = false;
        boolean found2 = false;
        boolean foundExtra = false;
        while (rids.hasNext()) {
            Rid rid = rids.next();
            int pageId = rid.getPageId();
            int slotId = rid.getSlotId();
            DataPage page = (DataPage) bf.getPage(pageId, DATA_PAGE_INDEX);
            Row row = page.getRow(slotId);
            String rowTitle = new String(removeTrailingBytes(row.movieTitle()), StandardCharsets.UTF_8).trim();
            if (rowTitle.equals(movieTitle1)) {
                found1 = true;
            } else if (rowTitle.equals(movieTitle2)) {
                found2 = true;
            } else if (rowTitle.equals(movieTitleExtra)) {
                foundExtra = true;
            }
            if (found1 && found2 && foundExtra) {
                break;
            }
            bf.unpinPage(pageId, DATA_PAGE_INDEX);
        }
        assertTrue(found1 && found2 && foundExtra,
                "Could not find the movies using Range Search using the movie title Btree in the Movies table");
    }

    @Test
    void testC4MovieId() {
        String movieId1 = "tt0000003";
        String movieId2 = "tt0000011";
        String movieIdExtra = "tt0000007";
        Iterator<Rid> rids = movieIdBtree.rangeSearch(movieId1, movieId2);
        boolean found1 = false;
        boolean found2 = false;
        boolean foundExtra = false;
        while (rids.hasNext()) {
            Rid rid = rids.next();
            int pageId = rid.getPageId();
            int slotId = rid.getSlotId();
            DataPage page = (DataPage) bf.getPage(pageId, DATA_PAGE_INDEX);
            Row row = page.getRow(slotId);
            String rowId = new String(removeTrailingBytes(row.movieId()), StandardCharsets.UTF_8).trim();
            if (rowId.equals(movieId1)) {
                found1 = true;
            } else if (rowId.equals(movieId2)) {
                found2 = true;
            } else if (rowId.equals(movieIdExtra)) {
                foundExtra = true;
            }
            if (found1 && found2 && foundExtra) {
                break;
            }
            bf.unpinPage(pageId, DATA_PAGE_INDEX);
        }
        assertTrue(found1 && found2 && foundExtra,
                "Could not find the movies using Range Search using the movie id Btree in the Movies table");
    }

    // @Test
    // void testP1() {
    // int[] ranges = { 1, 2, 4, 8, 16, 32, 64, 128, 256 };
    // long[] scanTableTimes = new long[ranges.length];
    // long[] scanTitleIndexTimes = new long[ranges.length];
    // for (int i = 0; i < ranges.length; i++) {
    // long startTableTime = System.nanoTime();
    // int j = 0;
    // int p = 0;
    // int r = 0;
    // while (j < ranges[i]) {
    // DataPage page = (DataPage) bf.getPage(p, DATA_PAGE_INDEX);
    // Row row = page.getRow(r);
    // if (row != null) {
    // j++;
    // r++;
    // } else {
    // bf.unpinPage(p, DATA_PAGE_INDEX);
    // p++;
    // }
    // }
    // long endTableTime = System.nanoTime();
    // scanTableTimes[i] = endTableTime - startTableTime;

    // String[] zerothAndRangeKey = movieTitleBtree.getZerothAndNthKeys(ranges[i] -
    // 1);

    // long startTitleTime = System.nanoTime();
    // List<Rid> rids = movieTitleBtree.rangeSearch(zerothAndRangeKey[0],
    // zerothAndRangeKey[1]);
    // for (Rid rid : rids) {
    // int pageId = rid.getPageId();
    // int slotId = rid.getSlotId();
    // DataPage page = (DataPage) bf.getPage(pageId, DATA_PAGE_INDEX);
    // Row row = page.getRow(slotId);
    // }
    // long endTitleTime = System.nanoTime();
    // scanTitleIndexTimes[i] = startTitleTime - endTitleTime;
    // }

    // SwingUtilities.invokeLater(() -> {
    // JFrame frame = new JFrame("P1: Query Execution Time Plot Using Title");
    // frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    // frame.setLayout(new GridLayout(2, 1));

    // frame.add(createExecutionTimeChart(ranges, scanTableTimes,
    // scanTitleIndexTimes));
    // frame.add(createRatioChart(ranges, scanTableTimes, scanTitleIndexTimes));

    // frame.pack();
    // frame.setVisible(true);
    // });
    // }

    @Test
    void testP2() {
        int[] ranges = { 1, 2, 4, 8, 16, 32, 64, 128, 256 };
        long[] scanTableTimes = new long[ranges.length];
        long[] scanIdIndexTimes = new long[ranges.length];
        for (int i = 0; i < ranges.length; i++) {
            bf.force();
            long startTableTime = System.nanoTime();
            int j = 0;
            int p = 0;
            int r = 0;
            DataPage page = (DataPage) bf.getPage(p, DATA_PAGE_INDEX);
            while (j < ranges[i]) {
                if (page == null) {
                    System.out.print(p);
                }
                Row row = page.getRow(r);
                if (row != null) {
                    j++;
                    r++;
                } else {
                    bf.unpinPage(p, DATA_PAGE_INDEX);
                    p++;
                    page = (DataPage) bf.getPage(p, DATA_PAGE_INDEX);
                    r = 0;
                }
            }
            long endTableTime = System.nanoTime();
            scanTableTimes[i] = endTableTime - startTableTime;

            String rangeKey = String.format("tt%07d", ranges[i]);
            String[] zerothAndRangeKey = { "tt0000001", rangeKey };

            long startIdTime = System.nanoTime();
            Iterator<Rid> rids = movieIdBtree.rangeSearch(zerothAndRangeKey[0], zerothAndRangeKey[1]);
            while (rids.hasNext()) {
                Rid rid = rids.next();
                int pageId = rid.getPageId();
                int slotId = rid.getSlotId();
                page = (DataPage) bf.getPage(pageId, DATA_PAGE_INDEX);
                Row row = page.getRow(slotId);
                bf.unpinPage(pageId, DATA_PAGE_INDEX);
            }
            long endIdTime = System.nanoTime();
            scanIdIndexTimes[i] = startIdTime - endIdTime;
        }

        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame("P2: Query Execution Time Plot Using Id");
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.setLayout(new GridLayout(2, 1));

            frame.add(createExecutionTimeChart(ranges, scanTableTimes, scanIdIndexTimes));
            frame.add(createRatioChart(ranges, scanTableTimes, scanIdIndexTimes));

            frame.pack();
            frame.setVisible(true);
        });
    }

    // @Test
    // void testP3Title() {
    // // BufferManager bf = new BufferManagerImpl(bufferSize);
    // // BTreeImpl<String, Rid> movieTitleBtree = new BTreeImpl<String, Rid>(bf,
    // // MOVIE_TITLE_INDEX_INDEX);
    // // Utilities.loadDataset(bf, bf.getFilePath(DATA_PAGE_INDEX));
    // // Utilities.createMovieTitleIndex(bf, movieTitleBtree);

    // // get Root page, but not unpinning it
    // int rootPageId = Integer.parseInt(bf.getRootPageId(MOVIE_TITLE_INDEX_INDEX));
    // Page root = bf.getPage(rootPageId, MOVIE_TITLE_INDEX_INDEX);

    // int[] ranges = { 1, 2, 4, 8, 16, 32, 64, 128, 256 };
    // long[] scanTableTimes = new long[ranges.length];
    // long[] scanTitleIndexTimes = new long[ranges.length];
    // for (int i = 0; i < ranges.length; i++) {
    // long startTableTime = System.nanoTime();
    // int j = 0;
    // int p = 0;
    // int r = 0;
    // while (j < ranges[i]) {
    // DataPage page = (DataPage) bf.getPage(p, DATA_PAGE_INDEX);
    // Row row = page.getRow(r);
    // if (row != null) {
    // j++;
    // r++;
    // } else {
    // bf.unpinPage(p, DATA_PAGE_INDEX);
    // p++;
    // }
    // }
    // long endTableTime = System.nanoTime();
    // scanTableTimes[i] = endTableTime - startTableTime;

    // String[] zerothAndRangeKey = movieTitleBtree.getZerothAndNthKeys(ranges[i] -
    // 1);

    // long startTitleTime = System.nanoTime();
    // List<Rid> rids = movieTitleBtree.rangeSearch(zerothAndRangeKey[0],
    // zerothAndRangeKey[1]);
    // for (Rid rid : rids) {
    // int pageId = rid.getPageId();
    // int slotId = rid.getSlotId();
    // DataPage page = (DataPage) bf.getPage(pageId, DATA_PAGE_INDEX);
    // Row row = page.getRow(slotId);
    // }
    // long endTitleTime = System.nanoTime();
    // scanTitleIndexTimes[i] = startTitleTime - endTitleTime;
    // }

    // SwingUtilities.invokeLater(() -> {
    // JFrame frame = new JFrame("P1: Query Execution Time Plot Using Title");
    // frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    // frame.setLayout(new GridLayout(2, 1));

    // frame.add(createExecutionTimeChart(ranges, scanTableTimes,
    // scanTitleIndexTimes));
    // frame.add(createRatioChart(ranges, scanTableTimes, scanTitleIndexTimes));

    // frame.pack();
    // frame.setVisible(true);
    // });
    // }

    @Test
    void testP3Id() {
        // BufferManager bf = new BufferManagerImpl(bufferSize);
        // BTreeImpl<String, Rid> movieIdBtree = new BTreeImpl<String, Rid>(bf,
        // MOVIE_ID_INDEX_PAGE_INDEX);
        // Utilities.loadDataset(bf, bf.getFilePath(DATA_PAGE_INDEX));
        // Utilities.createMovieIdIndexUsingBulkInsert(bf, movieIdBtree);

        // get Root page, but not unpinning it
        int rootPageId = Integer.parseInt(bf.getRootPageId(MOVIE_ID_INDEX_PAGE_INDEX));
        Page root = bf.getPage(rootPageId, MOVIE_ID_INDEX_PAGE_INDEX);

        int[] ranges = { 1, 2, 4, 8, 16, 32, 64, 128, 256 };
        long[] scanTableTimes = new long[ranges.length];
        long[] scanIdIndexTimes = new long[ranges.length];
        for (int i = 0; i < ranges.length; i++) {
            bf.force();
            long startTableTime = System.nanoTime();
            int j = 0;
            int p = 0;
            int r = 0;
            DataPage page = (DataPage) bf.getPage(p, DATA_PAGE_INDEX);
            while (j < ranges[i]) {
                Row row = page.getRow(r);
                if (row != null) {
                    j++;
                    r++;
                } else {
                    bf.unpinPage(p, DATA_PAGE_INDEX);
                    p++;
                    page = (DataPage) bf.getPage(p, DATA_PAGE_INDEX);
                    r = 0;
                }
            }
            long endTableTime = System.nanoTime();
            scanTableTimes[i] = endTableTime - startTableTime;

            String rangeKey = String.format("tt%07d", ranges[i]);
            String[] zerothAndRangeKey = { "tt0000001", rangeKey };

            long startIdTime = System.nanoTime();
            Iterator<Rid> rids = movieIdBtree.rangeSearch(zerothAndRangeKey[0], zerothAndRangeKey[1]);
            while (rids.hasNext()) {
                Rid rid = rids.next();
                int pageId = rid.getPageId();
                int slotId = rid.getSlotId();
                page = (DataPage) bf.getPage(pageId, DATA_PAGE_INDEX);
                Row row = page.getRow(slotId);
                bf.unpinPage(pageId, DATA_PAGE_INDEX);
            }
            long endIdTime = System.nanoTime();
            scanIdIndexTimes[i] = startIdTime - endIdTime;
        }

        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame("P2: Query Execution Time Plot Using Id");
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.setLayout(new GridLayout(2, 1));

            frame.add(createExecutionTimeChart(ranges, scanTableTimes, scanIdIndexTimes));
            frame.add(createRatioChart(ranges, scanTableTimes, scanIdIndexTimes));

            frame.pack();
            frame.setVisible(true);
        });
    }

    private static JPanel createExecutionTimeChart(int[] ranges, long[] scanTableTimes, long[] scanTitleIndexTimes) {
        XYSeries tableSeries = new XYSeries("Scan Table");
        XYSeries indexSeries = new XYSeries("Scan Index");

        for (int i = 0; i < ranges.length; i++) {
            tableSeries.add(ranges[i], scanTableTimes[i]);
            indexSeries.add(ranges[i], scanTitleIndexTimes[i]);
        }

        XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(tableSeries);
        dataset.addSeries(indexSeries);

        JFreeChart chart = ChartFactory.createXYLineChart(
                "Query Execution Time",
                "Selectivity",
                "Execution Time (ms)",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false);

        customizeChart(chart);
        return new ChartPanel(chart);
    }

    private static JPanel createRatioChart(int[] ranges, long[] scanTableTimes, long[] scanTitleIndexTimes) {
        XYSeries ratioSeries = new XYSeries("Execution Time Ratio (Table/Index)");

        for (int i = 0; i < ranges.length; i++) {
            double ratio = (double) scanTableTimes[i] / scanTitleIndexTimes[i];
            ratioSeries.add(ranges[i], ratio);
        }

        XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(ratioSeries);

        JFreeChart chart = ChartFactory.createXYLineChart(
                "Execution Time Ratio",
                "Selectivity",
                "Ratio (Table / Index)",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false);

        customizeChart(chart);
        return new ChartPanel(chart);
    }

    private static void customizeChart(JFreeChart chart) {
        XYPlot plot = chart.getXYPlot();
        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        renderer.setSeriesShapesVisible(0, true);
        renderer.setSeriesShapesVisible(1, true);
        plot.setRenderer(renderer);
    }
}
