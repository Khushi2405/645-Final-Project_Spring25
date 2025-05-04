package com.database.finalproject.queryExecution;

import static com.database.finalproject.constants.PageConstants.BNL_MOVIE_WORKED_ON_INDEX;
import static com.database.finalproject.constants.PageConstants.BNL_MOVIE_WORKED_ON_PEOPLE_INDEX;
import static com.database.finalproject.constants.PageConstants.MOVIES_DATA_PAGE_INDEX;
import static com.database.finalproject.constants.PageConstants.MOVIE_PERSON_DATA_PAGE_INDEX;
import static com.database.finalproject.constants.PageConstants.PEOPLE_DATA_PAGE_INDEX;
import static com.database.finalproject.constants.PageConstants.SAMPLE_RANGES_CSV;
import static com.database.finalproject.constants.PageConstants.WORKED_ON_DATA_PAGE_INDEX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.database.finalproject.buffermanager.BufferManagerImpl;
import com.database.finalproject.controller.UserController;
import com.database.finalproject.model.Comparator;
import com.database.finalproject.model.ProjectionType;
import com.database.finalproject.model.SelectionPredicate;
import com.database.finalproject.model.record.MoviePersonRecord;
import com.database.finalproject.model.record.MovieRecord;
import com.database.finalproject.model.record.MovieWorkedOnJoinRecord;
import com.database.finalproject.model.record.MovieWorkedOnPeopleJoinRecord;
import com.database.finalproject.model.record.PeopleRecord;
import com.database.finalproject.model.record.TitleNameRecord;
import com.database.finalproject.model.record.WorkedOnRecord;
import com.database.finalproject.queryplan.BNLJoinOperator;
import com.database.finalproject.queryplan.MaterializeOperator;
import com.database.finalproject.queryplan.ProjectionOperator;
import com.database.finalproject.queryplan.ScanOperator;
import com.database.finalproject.queryplan.SelectionOperator;

public class queryPerformanceTest {
    // @BeforeAll
    // static void setup() {
    // // Note: Please update the database_catalogue.txt to make the number of pages
    // of
    // // the respective table to 0 before running this.
    // // BufferManagerImpl bufferManager = new BufferManagerImpl(100000);
    // // Utilities.loadMoviesDataset(bufferManager, MOVIE_DATABASE_FILE);
    // // Utilities.loadPeopleDataset(bufferManager, PEOPLE_DATABASE_FILE);
    // // Utilities.loadWorkedOnDataset(bufferManager, WORKED_ON_DATABASE_FILE);
    // }

    @Test
    void performanceTest() throws IOException {
        List<String[]> sampleRanges = readRangesFromCSV(SAMPLE_RANGES_CSV);

        List<Double> selectivities = new ArrayList<>();
        List<AtomicInteger> measuredIOs = new ArrayList<>();
        List<Double> estimatedIOs = new ArrayList<>();

        for (int i = 1; i < sampleRanges.size(); i++) {
            String[] range = sampleRanges.get(i);
            String startKey = range[0].trim();
            String endKey = range[1].trim();

            if (startKey.compareTo(endKey) > 0) {
                String temp = startKey;
                startKey = endKey;
                endKey = temp;
            }
            int bufferSize = 100000;

            // bufferManager.resetIOCount();
            UserController uc = new UserController(bufferSize);
            Map<String, Object> map = (Map<String, Object>) uc.runQuery(startKey, endKey);
            AtomicInteger ioCount = (AtomicInteger) map.get("iocount");
            ioCount.addAndGet(19385); // materialized pages cost
            Long movieSelection = (Long) map.get("movieSelection");

            double measuredSelectivity = (movieSelection) / (double) getTotalMoviesCount();
            double estimatedIO = estimateIO(bufferSize, measuredSelectivity);

            selectivities.add(measuredSelectivity);
            measuredIOs.add(ioCount);
            estimatedIOs.add(estimatedIO);

        }
        plotAndSaveIOGraph(selectivities, measuredIOs, estimatedIOs, "query_performance_chart.png");
    }

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

    private static int getTotalMoviesCount() {
        // This should return the total number of records in the Movies table
        return 5632943;
    }

    private static double estimateIO(int bufferSize, double movieSelectivity) {
        long totalMoviePages = 53648;
        long workedOnPages = 486105;
        long peoplePages = 410238;

        long selectedMoviePages = (long) (Math.ceil(movieSelectivity * getTotalMoviesCount() / 105));

        long projectedWorkedOnPages = (long) (19385); // materialized pages

        // Materialize projected selection: write
        long workedOnMaterializeIO = projectedWorkedOnPages;

        // First join (Movies ⨝ WorkedOn)
        int blockSize = (bufferSize - 4) / 2;
        double join1OuterCost = selectedMoviePages;
        double join1InnerCost = (projectedWorkedOnPages * (Math.ceil(selectedMoviePages / blockSize)));

        long join1OutputPages = selectedMoviePages;

        // Second join (Join1Result ⨝ People)
        double join2OuterCost = selectedMoviePages;
        double join2InnerCost = (peoplePages * (Math.ceil(join1OutputPages / blockSize)));

        // Total estimated I/O:2ws
        return totalMoviePages // read movies
                + workedOnPages // scan workedOn
                + workedOnMaterializeIO // materialized write
                + join1OuterCost
                + join1InnerCost // join reads both sides
                + join2OuterCost
                + join2InnerCost; // second join reads both sides
    }

    private static void plotAndSaveIOGraph(List<Double> selectivities,
            List<AtomicInteger> measuredIOs,
            List<Double> estimatedIOs,
            String outputPath) throws IOException {
        XYSeries measuredSeries = new XYSeries("Measured I/O");
        XYSeries estimatedSeries = new XYSeries("Estimated I/O");

        for (int i = 0; i < selectivities.size(); i++) {
            measuredSeries.add(selectivities.get(i), measuredIOs.get(i));
            estimatedSeries.add(selectivities.get(i), estimatedIOs.get(i));
        }

        XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(measuredSeries);
        dataset.addSeries(estimatedSeries);

        JFreeChart chart = ChartFactory.createXYLineChart(
                "Measured vs Estimated I/O Cost",
                "Selectivity",
                "I/O Cost",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false);

        ChartUtils.saveChartAsPNG(new File(outputPath), chart, 1000, 700);
    }
}
