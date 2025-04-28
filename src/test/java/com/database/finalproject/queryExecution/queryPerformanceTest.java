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
import org.junit.jupiter.api.Test;

import com.database.finalproject.buffermanager.BufferManagerImpl;
import com.database.finalproject.model.Comparator;
import com.database.finalproject.model.ProjectionType;
import com.database.finalproject.queryplan.BNLJoinOperator;
import com.database.finalproject.queryplan.MaterializeOperator;
import com.database.finalproject.queryplan.ProjectionOperator;
import com.database.finalproject.queryplan.ScanOperator;
import com.database.finalproject.queryplan.SelectionOperator;

public class queryPerformanceTest {
    @Test
    void performanceTest() throws IOException {
        List<String[]> sampleRanges = readRangesFromCSV(SAMPLE_RANGES_CSV);

        List<Double> selectivities = new ArrayList<>();
        List<Integer> measuredIOs = new ArrayList<>();
        List<Integer> estimatedIOs = new ArrayList<>();

        for (String[] range : sampleRanges) {
            String startRange = range[0];
            String endRange = range[1];
            int bufferSize = 100;

            BufferManagerImpl bufferManager = new BufferManagerImpl(bufferSize);

            bufferManager.resetIOCount();
            // Query Plan Construction
            ScanOperator movieScan = new ScanOperator(bufferManager, MOVIES_DATA_PAGE_INDEX);
            SelectionOperator movieSelection = new SelectionOperator(movieScan, 1, startRange,
                    Comparator.GREATER_THAN_OR_EQUALS);
            SelectionOperator movieSelection2 = new SelectionOperator(movieSelection, 1, endRange,
                    Comparator.LESS_THAN_OR_EQUALS);

            ScanOperator workedOnScan = new ScanOperator(bufferManager, WORKED_ON_DATA_PAGE_INDEX);
            SelectionOperator workedOnSelection = new SelectionOperator(workedOnScan, 2, "director", Comparator.EQUALS);
            MaterializeOperator materialize = new MaterializeOperator(workedOnSelection, bufferManager, 2);
            // ScanOperator materializedScan = new ScanOperator(bufferManager, 2);

            BNLJoinOperator join1 = new BNLJoinOperator(movieSelection2, materialize, 0, 0, (bufferSize - 4) / 2,
                    bufferManager, BNL_MOVIE_WORKED_ON_INDEX);

            ScanOperator peopleScan = new ScanOperator(bufferManager, PEOPLE_DATA_PAGE_INDEX); // assuming index 3 for
                                                                                               // people

            BNLJoinOperator join2 = new BNLJoinOperator(join1, peopleScan, 1, 0, (bufferSize - 4) / 4, bufferManager,
                    BNL_MOVIE_WORKED_ON_PEOPLE_INDEX);
            ProjectionOperator projection = new ProjectionOperator(join2, ProjectionType.PROJECTION_ON_FINAL_JOIN);

            projection.open();
            int recordCount = 0;
            while (projection.next() != null) {
                recordCount++;
            }
            projection.close();

            double measuredSelectivity = recordCount / (double) getTotalMoviesCount();
            int measuredIO = bufferManager.getIOCount();
            int estimatedIO = estimateIO(bufferSize, measuredSelectivity);

            selectivities.add(measuredSelectivity);
            measuredIOs.add(measuredIO);
            estimatedIOs.add(estimatedIO);
        }
        plotAndSaveIOGraph(selectivities, measuredIOs, estimatedIOs, "performance_chart.png");
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

    private static int estimateIO(int bufferSize, double selectivity) {
        int moviesPages = (int) (selectivity * 53648); // mock: assume 100 pages in total
        int workedOnPages = 486153; // constant size
        int intermediatePages = moviesPages * workedOnPages / bufferSize;
        return moviesPages + workedOnPages + intermediatePages * 2;
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
