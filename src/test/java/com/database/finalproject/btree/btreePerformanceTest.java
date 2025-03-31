package com.database.finalproject.btree;

import static com.database.finalproject.constants.PageConstants.MOVIE_ID_INDEX_PAGE_INDEX;
import static com.database.finalproject.constants.PageConstants.MOVIE_TITLE_INDEX_INDEX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;

import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.buffermanager.BufferManagerImpl;
import com.database.finalproject.controller.UserController;
import com.database.finalproject.model.DataPage;
import com.database.finalproject.model.Page;
import com.database.finalproject.model.Rid;
import com.database.finalproject.model.Row;
import com.database.finalproject.repository.Utilities;

public class btreePerformanceTest {
    // private static BufferManager bf;
    // private static BTree<String, Rid> movieTitleIndex;
    // private static BTree<String, Rid> movieIdIndex;
    // private static List<Row> moviesTable;
    private static UserController uc;

    @BeforeAll
    static void setUp() {
        uc = new UserController(5);
        // bf = new BufferManagerImpl(5);
        // movieIdIndex = new BTreeImpl(bf, MOVIE_ID_INDEX_PAGE_INDEX); // Initialize B+
        // Tree for titles
        // movieTitleIndex = new BTreeImpl(bf, MOVIE_TITLE_INDEX_INDEX); // Initialize
        // B+ Tree for movie IDs

        // Utilities.createMovieTitleIndex(bf, movieTitleIndex);
        // Utilities.createMovieIdIndex(bf, movieIdIndex);

        // Load full Movies table for sequential scan
        // moviesTable = loadMoviesTable();
    }

    @Test
    void testP1_RangeQueryPerformance_TitleIndex() throws IOException {
        List<Double> selectivity = new ArrayList<>();
        List<Double> timeScan = new ArrayList<>();
        List<Double> timeIndex = new ArrayList<>();
        List<Double> ratio = new ArrayList<>();

        // TODO: get increasingly large range for testing [start/end keys?]
        // Selectivity range from 1% to 50% (sample values)
        // for (double percent = 0.01; percent <= 0.5; percent += 0.05) {
        // int numRows = (int) (moviesTable.size() * percent);

        // selectivity.add(percent);

        String startkey = "A Hard Wash", endkey = "The Boxing Kangaroo";
        // Method 1: Sequential Scan
        long startScan = System.nanoTime();
        List<Row> result = uc.rangeSearchSequentialScan(startkey, endkey, MOVIE_TITLE_INDEX_INDEX);
        long endScan = System.nanoTime();
        timeScan.add((endScan - startScan) / 1e6); // Convert ns to ms

        // Method 2: Index-based Search (Title Index)
        long startIndex = System.nanoTime();
        List<Row> result2 = uc.rangeSearchMovieTitle(startkey, endkey);
        long endIndex = System.nanoTime();
        timeIndex.add((endIndex - startIndex) / 1e6);

        // Compute ratio (Scan Time / Index Time)
        ratio.add(timeScan.get(timeScan.size() - 1) / timeIndex.get(timeIndex.size()
                - 1));
        // }

        // Generate plots
        generateChart(selectivity, timeScan, timeIndex, "Query Selectivity", "Query Execution Time (ms)",
                "Range Query Execution Time - Title Index",
                "method1_scan_vs_method2_index.png");
        generateChart(selectivity, ratio, "Query Selectivity", "Scan Time / Index Time Ratio",
                "Ratio of Execution Times", "scan_vs_index_ratio.png");
    }

    @Test
    void testP2_RangeQueryPerformance_MovieIdIndex() throws IOException {
        List<Double> selectivity = new ArrayList<>();
        List<Double> timeIndexMovieId = new ArrayList<>();
        List<Double> timeScan = new ArrayList<>();
        List<Double> ratio = new ArrayList<>();

        // TODO: get increasingly large range for testing [start/end keys?]

        // for (double percent = 0.01; percent <= 0.5; percent += 0.05) {
        // selectivity.add(percent);

        String startkey = "tt0000082", endkey = "tt0000131";
        // Method 1: Sequential Scan
        long startScan = System.nanoTime();
        List<Row> result = uc.rangeSearchSequentialScan(null, null, MOVIE_TITLE_INDEX_INDEX);
        long endScan = System.nanoTime();
        timeScan.add((endScan - startScan) / 1e6); // Convert ns to ms

        // Method 2: Index-based Search (Movie ID Index)
        long startIndex = System.nanoTime();
        List<Row> result2 = uc.rangeSearchMovieId(null, null);

        long endIndex = System.nanoTime();
        timeIndexMovieId.add((endIndex - startIndex) / 1e6);
        // }

        // Compute ratio (Scan Time / Index Time)
        ratio.add(timeScan.get(timeScan.size() - 1) / timeIndexMovieId.get(timeIndexMovieId.size()
                - 1));
        // }

        // Generate plots
        generateChart(selectivity, timeScan, timeIndexMovieId, "Query Selectivity", "Query Execution Time (ms)",
                "Range Query Execution Time - ID Index",
                "method1_scan_vs_method2_index.png");
        generateChart(selectivity, ratio, "Query Selectivity", "Scan Time / Index Time Ratio",
                "Ratio of Execution Times", "scan_vs_index_ratio.png");

        // generateChart(selectivity, timeIndexMovieId, "Query Selectivity", "Query
        // Execution Time (ms)",
        // "Movie ID Index vs. Title Index", "index_movie_id_vs_title.png");
    }

    private void generateChart(List<Double> xData, List<Double> yData1,
            List<Double> yData2, String xLabel,
            String yLabel, String title, String filename) throws IOException {
        XYChart chart = new XYChartBuilder().width(800).height(600).title(title).xAxisTitle(xLabel).yAxisTitle(yLabel)
                .build();
        chart.addSeries("Sequential Scan", xData, yData1);
        chart.addSeries("Index-Based Search", xData, yData2);
        BitmapEncoder.saveBitmap(chart, filename, BitmapEncoder.BitmapFormat.PNG);
    }

    private void generateChart(List<Double> xData, List<Double> yData, String xLabel, String yLabel, String title,
            String filename) throws IOException {
        XYChart chart = new XYChartBuilder().width(800).height(600).title(title).xAxisTitle(xLabel).yAxisTitle(yLabel)
                .build();
        chart.addSeries("Ratio", xData, yData);
        BitmapEncoder.saveBitmap(chart, filename, BitmapEncoder.BitmapFormat.PNG);
    }
}
