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
    @BeforeAll
    static void setup() {
        // Note: Please update the database_catalogue.txt to make the number of pages of
        // the respective table to 0 before running this.
        // BufferManagerImpl bufferManager = new BufferManagerImpl(100000);
        // Utilities.loadMoviesDataset(bufferManager, MOVIE_DATABASE_FILE);
        // Utilities.loadPeopleDataset(bufferManager, PEOPLE_DATABASE_FILE);
        // Utilities.loadWorkedOnDataset(bufferManager, WORKED_ON_DATABASE_FILE);
    }

    @Test
    void performanceTest() throws IOException {
        List<String[]> sampleRanges = readRangesFromCSV(SAMPLE_RANGES_CSV);

        List<Double> selectivities = new ArrayList<>();
        List<AtomicInteger> measuredIOs = new ArrayList<>();
        List<Integer> estimatedIOs = new ArrayList<>();

        for (int i = 1; i < sampleRanges.size(); i++) {
            String[] range = sampleRanges.get(i);
            String startKey = range[0].trim();
            String endKey = range[1].trim();

            if (startKey.compareTo(endKey) > 0) {
                String temp = startKey;
                startKey = endKey;
                endKey = temp;
            }
            int bufferSize = 1000;

            BufferManagerImpl bufferManager = new BufferManagerImpl(bufferSize);

            bufferManager.resetIOCount();
            // Query Plan Construction
            ScanOperator<MovieRecord> movieScan = new ScanOperator<>(bufferManager, MOVIES_DATA_PAGE_INDEX);

            List<SelectionPredicate> moviePredicates = new ArrayList<>();
            moviePredicates.add(new SelectionPredicate(1, startKey, Comparator.GREATER_THAN_OR_EQUALS));
            moviePredicates.add(new SelectionPredicate(1, endKey, Comparator.LESS_THAN_OR_EQUALS));
            SelectionOperator<MovieRecord> movieSelection = new SelectionOperator<>(movieScan, moviePredicates);

            ScanOperator<WorkedOnRecord> workedOnScan = new ScanOperator<>(bufferManager, WORKED_ON_DATA_PAGE_INDEX);

            List<SelectionPredicate> workedOnPredicates = new ArrayList<>();
            workedOnPredicates.add(new SelectionPredicate(2, "director", Comparator.EQUALS));
            SelectionOperator<WorkedOnRecord> workedOnSelection = new SelectionOperator<>(workedOnScan,
                    workedOnPredicates);

            ProjectionOperator<WorkedOnRecord, MoviePersonRecord> workedOnProjection = new ProjectionOperator<>(
                    workedOnSelection, ProjectionType.PROJECTION_ON_WORKED_ON);

            MaterializeOperator<MoviePersonRecord> workedOnMaterialized = new MaterializeOperator<>(workedOnProjection,
                    bufferManager, MOVIE_PERSON_DATA_PAGE_INDEX);

            BNLJoinOperator<MovieRecord, MoviePersonRecord, MovieWorkedOnJoinRecord> join1 = new BNLJoinOperator<>(
                    movieSelection, workedOnMaterialized, 0, 0, (bufferSize - 4) / 2,
                    bufferManager, BNL_MOVIE_WORKED_ON_INDEX);

            ScanOperator<PeopleRecord> peopleScan = new ScanOperator<>(bufferManager, PEOPLE_DATA_PAGE_INDEX);

            BNLJoinOperator<MovieWorkedOnJoinRecord, PeopleRecord, MovieWorkedOnPeopleJoinRecord> join2 = new BNLJoinOperator<>(
                    join1, peopleScan, 1, 0, (bufferSize - 4) / 4, bufferManager,
                    BNL_MOVIE_WORKED_ON_PEOPLE_INDEX);
            ProjectionOperator<MovieWorkedOnPeopleJoinRecord, TitleNameRecord> projection = new ProjectionOperator<>(
                    join2, ProjectionType.PROJECTION_ON_FINAL_JOIN);

            projection.open();

            // Prepare Excel file
            Workbook workbook = new XSSFWorkbook();
            Sheet sheet = workbook.createSheet("QueryResults_" + (i));
            int rowNum = 0;
            Row headerRow = sheet.createRow(rowNum++);
            headerRow.createCell(0).setCellValue("Title");
            headerRow.createCell(1).setCellValue("Name");

            TitleNameRecord output;
            int recordCount = 0;
            while ((output = projection.next()) != null) {
                recordCount++;
                String title = new String(output.title()).trim();
                String name = new String(output.name()).trim();

                // Print to console
                // System.out.println(title + "," + name);

                // Write to Excel
                Row row = sheet.createRow(rowNum++);
                row.createCell(0).setCellValue(title);
                row.createCell(1).setCellValue(name);
            }
            projection.close();

            try (FileOutputStream fileOut = new FileOutputStream("query_output.xlsx")) {
                workbook.write(fileOut);
                workbook.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            // Note: Uncomment to create materialized table
            // bufferManager.resetMaterializedTable();

            double measuredSelectivity = recordCount / (double) getTotalMoviesCount();
            AtomicInteger measuredIO = bufferManager.getIoCounter();
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
            List<AtomicInteger> measuredIOs,
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
