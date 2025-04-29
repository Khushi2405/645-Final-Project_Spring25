package com.database.finalproject.queryExecution;

import static com.database.finalproject.constants.PageConstants.BNL_MOVIE_WORKED_ON_INDEX;
import static com.database.finalproject.constants.PageConstants.BNL_MOVIE_WORKED_ON_PEOPLE_INDEX;
import static com.database.finalproject.constants.PageConstants.MOVIES_DATA_PAGE_INDEX;
import static com.database.finalproject.constants.PageConstants.PEOPLE_DATA_PAGE_INDEX;
import static com.database.finalproject.constants.PageConstants.SAMPLE_RANGES_CSV;
import static com.database.finalproject.constants.PageConstants.WORKED_ON_DATA_PAGE_INDEX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.database.finalproject.buffermanager.BufferManagerImpl;
import com.database.finalproject.model.Comparator;
import com.database.finalproject.model.ProjectionType;
import com.database.finalproject.model.SelectionPredicate;
import com.database.finalproject.queryplan.BNLJoinOperator;
import com.database.finalproject.queryplan.MaterializeOperator;
import com.database.finalproject.queryplan.ProjectionOperator;
import com.database.finalproject.queryplan.ScanOperator;
import com.database.finalproject.queryplan.SelectionOperator;

public class queryPerformanceTest {
    @BeforeAll
    void setup() {
        // Note: Please update the database_catalogue.txt to make the number of pages of
        // the respective table to 0 before running this.
        // BufferManagerImpl bufferManager = new BufferManagerImpl(100);
        // Utilities.loadMoviesDataset(bufferManager, MOVIE_DATABASE_FILE);
        // Utilities.loadPeopleDataset(bufferManager, PEOPLE_DATABASE_FILE);
        // Utilities.loadWorkedOnDataset(bufferManager, WORKED_ON_DATABASE_FILE);
    }

    @Test
    void performanceTest() throws IOException {
        List<String[]> sampleRanges = readRangesFromCSV(SAMPLE_RANGES_CSV);

        List<Double> selectivities = new ArrayList<>();
        List<Integer> measuredIOs = new ArrayList<>();
        List<Integer> estimatedIOs = new ArrayList<>();

        for (int i = 0; i < sampleRanges.size(); i++) {
            String[] range = sampleRanges.get(i);
            String startKey = range[0].trim();
            String endKey = range[1].trim();

            if (startKey.compareTo(endKey) > 0) {
                String temp = startKey;
                startKey = endKey;
                endKey = temp;
            }
            int bufferSize = 100;

            BufferManagerImpl bufferManager = new BufferManagerImpl(bufferSize);

            bufferManager.resetIOCount();
            // Query Plan Construction
            ScanOperator movieScan = new ScanOperator(bufferManager, MOVIES_DATA_PAGE_INDEX);

            List<SelectionPredicate> moviePredicates = new ArrayList<>();
            moviePredicates.add(new SelectionPredicate(1, startKey, Comparator.GREATER_THAN_OR_EQUALS));
            moviePredicates.add(new SelectionPredicate(1, endKey, Comparator.LESS_THAN_OR_EQUALS));
            SelectionOperator movieSelection = new SelectionOperator(movieScan, moviePredicates);

            ScanOperator workedOnScan = new ScanOperator(bufferManager, WORKED_ON_DATA_PAGE_INDEX);

            List<SelectionPredicate> workedOnPredicates = new ArrayList<>();
            workedOnPredicates.add(new SelectionPredicate(2, "director", Comparator.EQUALS));
            SelectionOperator workedOnSelection = new SelectionOperator(workedOnScan, workedOnPredicates);

            MaterializeOperator materialize = new MaterializeOperator(workedOnSelection, bufferManager, 2);

            BNLJoinOperator join1 = new BNLJoinOperator(movieSelection, materialize, 0, 0, (bufferSize - 4) / 2,
                    bufferManager, BNL_MOVIE_WORKED_ON_INDEX);

            ScanOperator peopleScan = new ScanOperator(bufferManager, PEOPLE_DATA_PAGE_INDEX);

            BNLJoinOperator join2 = new BNLJoinOperator(join1, peopleScan, 1, 0, (bufferSize - 4) / 4, bufferManager,
                    BNL_MOVIE_WORKED_ON_PEOPLE_INDEX);
            ProjectionOperator projection = new ProjectionOperator(join2, ProjectionType.PROJECTION_ON_FINAL_JOIN);

            projection.open();
            int recordCount = 0;
            while (projection.next() != null) {
                recordCount++;
            }
            projection.close();

            // Note: Uncomment to create materialized table
            // bufferManager.resetMaterializedTable();

            double measuredSelectivity = recordCount / (double) getTotalMoviesCount();
            int measuredIO = bufferManager.getIOCount();
            int estimatedIO = (int) estimateIO(bufferSize, measuredSelectivity);

            selectivities.add(measuredSelectivity);
            measuredIOs.add(measuredIO);
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
        return 53648 * 105;
    }

    private static long estimateIO(int bufferSize, double movieSelectivity) {
        long totalMoviePages = 53648;
        long workedOnPages = 486153;
        long peoplePages = 410238;
        // long selectedWorkedOnPages = 0; // const to be calc offline

        long selectedMoviePages = (long) (movieSelectivity * totalMoviePages);

        long projectedWorkedOnPages = (long) (3391); // materialized pages

        // Materialize projected selection: write + read
        long workedOnMaterializeIO = 2 * projectedWorkedOnPages;

        // First join (Movies ⨝ WorkedOn)
        long join1Cost = selectedMoviePages * projectedWorkedOnPages / bufferSize;

        long join1OutputPages = join1Cost; // TODO: join 1 OP pages??

        // Second join (Join1Result ⨝ People)
        long join2Cost = join1OutputPages * peoplePages / bufferSize;

        // TODO: check join cost
        // Total estimated I/O:2ws
        return totalMoviePages // read movies
                + workedOnPages // scan workedOn
                + workedOnMaterializeIO // materialized write+read
                + join1Cost * 2 // join reads both sides
                + peoplePages // read people table
                + join2Cost * 2; // second join reads both sides
    }

    private static void plotAndSaveIOGraph(List<Double> selectivities,
            List<Integer> measuredIOs,
            List<Integer> estimatedIOs,
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
