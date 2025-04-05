package com.database.finalproject.btree;

import static com.database.finalproject.constants.PageConstants.ATTR_TYPE_ID;
import static com.database.finalproject.constants.PageConstants.ATTR_TYPE_TITLE;
import static com.database.finalproject.constants.PageConstants.DATABASE_FILE;
import static com.database.finalproject.constants.PageConstants.DATA_PAGE_INDEX;
import static com.database.finalproject.constants.PageConstants.MOVIE_ID_INDEX_PAGE_INDEX;
import static com.database.finalproject.constants.PageConstants.MOVIE_TITLE_INDEX_INDEX;
import static com.database.finalproject.constants.PageConstants.PADDING_BYTE;
import static com.database.finalproject.constants.PageConstants.PAGE_SIZE;
import static com.database.finalproject.constants.PageConstants.SAMPLE_RANGES_CSV;
import static com.database.finalproject.constants.PageConstants.removeTrailingBytes;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.List;

import javax.swing.*;
import java.awt.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtils;
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

    // for the sample index files in the repository
    private static long TOTAL_RECORDS_ID = 10000 * 105;
    private static long TOTAL_RECORDS_TITLE = 10000 * 105;

    @BeforeAll
    static void setup() {
        int bufferSize = 5;
        bf = new BufferManagerImpl(bufferSize);
        movieIdBtree = new BTreeImpl(bf, MOVIE_ID_INDEX_PAGE_INDEX);
        movieTitleBtree = new BTreeImpl(bf, MOVIE_TITLE_INDEX_INDEX);

        // To create index files/binary file of dataset uncomment the below lines.
        // Note: reset database_catalogue.txt values to 0,-1 for index 1,2 for each line
        // Utilities.loadDataset(bf, DATABASE_FILE);
        // Utilities.createMovieIdIndex(bf, movieIdBtree);
        // Utilities.createMovieTitleIndex(bf, movieTitleBtree);

        // to get the total records in the index
        // TOTAL_RECORDS_ID = movieIdBtree.printKeys();
        // TOTAL_RECORDS_TITLE = movieIdBtree.printKeys();
    }

    @AfterEach
    void cleanup() {
        bf.force();
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
    void testP2_random_title_range() throws IOException {
        Random rand = new Random();
        List<String[]> sampleRanges = readRangesFromCSV(SAMPLE_RANGES_CSV);

        int[] selectivities = new int[sampleRanges.size()]; // Number of selectivities to test
        long[] queryTimes = new long[sampleRanges.size()];
        long[] queryTimesIndex = new long[sampleRanges.size()];
        long[] rowsReturned = new long[sampleRanges.size()];

        for (int i = 0; i < sampleRanges.size(); i++) {
            bf.force();
            String[] range = sampleRanges.get(i);
            String startKey = range[0].trim();
            String endKey = range[1].trim();

            if (startKey.compareTo(endKey) > 0) {
                String temp = startKey;
                startKey = endKey;
                endKey = temp;
            }

            long startTableTime = System.nanoTime();
            List<Row> result = rangeSearchSequentialScan(startKey, endKey, ATTR_TYPE_TITLE);
            long endTableTime = System.nanoTime();
            queryTimes[i] = (endTableTime - startTableTime);
            rowsReturned[i] = result.size();

            long startIdTime = System.nanoTime();
            Iterator<Rid> rids = movieTitleBtree.rangeSearch(startKey, endKey);
            List<Row> result2 = new ArrayList<>();
            while (rids.hasNext()) {
                Rid rid = rids.next();
                int pageId = rid.getPageId();
                int slotId = rid.getSlotId();
                DataPage page = (DataPage) bf.getPage(pageId, DATA_PAGE_INDEX);
                Row row = page.getRow(slotId);
                result2.add(row);
                bf.unpinPage(pageId, DATA_PAGE_INDEX);
            }
            long endIdTime = System.nanoTime();
            queryTimesIndex[i] = (endIdTime - startIdTime);
            // rowsReturned[i] = result2.size();

            double selectivity = (double) rowsReturned[i] / TOTAL_RECORDS_TITLE * 100;
            selectivities[i] = (int) selectivity;
        }

        // Create charts
        JFreeChart execTimeChart = createExecutionTimeChart(selectivities, queryTimes, queryTimesIndex);
        JFreeChart ratioChart = createRatioChart(selectivities, queryTimes, queryTimesIndex);

        // Write PNGs (test-safe!)
        ChartUtils.saveChartAsPNG(new File("execution_time_chart_p2_random_title_range.png"), execTimeChart, 800, 600);
        ChartUtils.saveChartAsPNG(new File("execution_ratio_chart_p2_random_title_range.png"), ratioChart, 800, 600);
    }

    @Test
    void testP2_random_id_range() throws IOException {
        Random rand = new Random();

        int[] selectivities = new int[10]; // Number of selectivities to test
        long[] queryTimes = new long[selectivities.length];
        long[] queryTimesIndex = new long[selectivities.length];
        long[] rowsReturned = new long[selectivities.length];

        for (int i = 0; i < selectivities.length; i++) {
            bf.force();

            // Randomize start and end keys from the index
            String startKey = generateRandomKeyForId(rand);
            String endKey = generateRandomKeyForId(rand);
            if (startKey.compareTo(endKey) > 0) {
                String temp = startKey;
                startKey = endKey;
                endKey = temp;
            }

            long startTableTime = System.nanoTime();
            List<Row> result = rangeSearchSequentialScan(startKey, endKey, ATTR_TYPE_ID);
            long endTableTime = System.nanoTime();
            queryTimes[i] = (endTableTime - startTableTime);
            rowsReturned[i] = result.size();

            long startIdTime = System.nanoTime();
            Iterator<Rid> rids = movieIdBtree.rangeSearch(startKey, endKey);
            List<Row> result2 = new ArrayList<>();
            while (rids.hasNext()) {
                Rid rid = rids.next();
                int pageId = rid.getPageId();
                int slotId = rid.getSlotId();
                DataPage page = (DataPage) bf.getPage(pageId, DATA_PAGE_INDEX);
                Row row = page.getRow(slotId);
                result2.add(row);
                bf.unpinPage(pageId, DATA_PAGE_INDEX);
            }
            long endIdTime = System.nanoTime();
            queryTimesIndex[i] = (endIdTime - startIdTime);
            // rowsReturned[i] = result2.size();

            double selectivity = (double) rowsReturned[i] / TOTAL_RECORDS_ID * 100;
            selectivities[i] = (int) selectivity;
        }

        // Create charts
        JFreeChart execTimeChart = createExecutionTimeChart(selectivities, queryTimes, queryTimesIndex);
        JFreeChart ratioChart = createRatioChart(selectivities, queryTimes, queryTimesIndex);

        // Write PNGs (test-safe!)
        ChartUtils.saveChartAsPNG(new File("execution_time_chart_p2_random_id_range.png"), execTimeChart, 800, 600);
        ChartUtils.saveChartAsPNG(new File("execution_ratio_chart_p2_random_id_range.png"), ratioChart, 800, 600);
    }

    @Test
    void testP2() throws IOException {
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
            scanIdIndexTimes[i] = endIdTime - startIdTime;
        }

        // Create charts
        JFreeChart execTimeChart = createExecutionTimeChart(ranges, scanTableTimes, scanIdIndexTimes);
        JFreeChart ratioChart = createRatioChart(ranges, scanTableTimes, scanIdIndexTimes);

        // Write PNGs (test-safe!)
        ChartUtils.saveChartAsPNG(new File("execution_time_chart_p2_id.png"), execTimeChart, 800, 600);
        ChartUtils.saveChartAsPNG(new File("execution_ratio_chart_p2_id.png"), ratioChart, 800, 600);

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
    void testP3Id() throws IOException {
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
            scanIdIndexTimes[i] = endIdTime - startIdTime;
        }

        // Create charts
        JFreeChart execTimeChart = createExecutionTimeChart(ranges, scanTableTimes, scanIdIndexTimes);
        JFreeChart ratioChart = createRatioChart(ranges, scanTableTimes, scanIdIndexTimes);

        // Write PNGs (test-safe!)
        ChartUtils.saveChartAsPNG(new File("execution_time_chart_p3_id.png"), execTimeChart, 800, 600);
        ChartUtils.saveChartAsPNG(new File("execution_ratio_chart_p3_id.png"), ratioChart, 800, 600);
    }

    private static JFreeChart createExecutionTimeChart(int[] ranges, long[] scanTableTimes,
            long[] scanTitleIndexTimes) {
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
                "Execution Time",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false);

        customizeChart(chart);
        // return new ChartPanel(chart);
        return chart;
    }

    private static JFreeChart createRatioChart(int[] ranges, long[] scanTableTimes, long[] scanTitleIndexTimes) {
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
        // return new ChartPanel(chart);
        return chart;
    }

    private static void customizeChart(JFreeChart chart) {
        XYPlot plot = chart.getXYPlot();
        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        renderer.setSeriesShapesVisible(0, true);
        renderer.setSeriesShapesVisible(1, true);
        plot.setRenderer(renderer);
    }

    // Helper method to generate a random key
    private String generateRandomKeyForId(Random rand) {
        int randomId = rand.nextInt((int) TOTAL_RECORDS_ID); // Assume ID is an integer from 0 to 1,000,000
        return String.format("tt%07d", randomId); // Example format: tt0001234
    }

    private List<Row> rangeSearchSequentialScan(String startKey, String endKey, int attrType) {
        List<Row> movies = new ArrayList<>();
        int dataPageId = 0;
        while (true) {
            Page currPage = bf.getPage(dataPageId, DATA_PAGE_INDEX);
            if (currPage == null)
                break;
            for (int i = 0; i < 105; i++) {
                Row row = ((DataPage) currPage).getRow(i);
                if (row == null)
                    break;

                byte[] key = attrType == ATTR_TYPE_ID ? row.movieId() : row.movieTitle();
                if (new String(removeTrailingBytes(key)).compareTo(startKey) >= 0
                        && new String(removeTrailingBytes(key)).compareTo(endKey) <= 0) {
                    movies.add(row);
                }
            }
            bf.unpinPage(dataPageId, DATA_PAGE_INDEX);
            dataPageId++;
        }
        return movies;
    }

    // Method to read start and end keys from a CSV file
    private List<String[]> readRangesFromCSV(String filePath) throws IOException {
        List<String[]> ranges = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Split the line by commas and store the pair
                String[] keys = line.split(",");
                if (keys.length == 2) {
                    ranges.add(keys);
                }
            }
        }
        return ranges;
    }
}
