package com.database.finalproject;

import com.database.finalproject.btree.BTreeImpl;
import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.buffermanager.BufferManagerImpl;
import com.database.finalproject.controller.UserController;
import com.database.finalproject.model.*;
import com.database.finalproject.model.record.*;
import com.database.finalproject.queryplan.*;
import com.database.finalproject.repository.Utilities;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileOutputStream;
import java.util.List;

import static com.database.finalproject.constants.PageConstants.*;


@SpringBootApplication
public class DatabaseFinalProjectApplication {

	public static void main(String[] args) {
		SpringApplication.run(DatabaseFinalProjectApplication.class, args);
//		UserController controller = new UserController(100);
//		controller.searchMovieId("Episode dated 2 February 1990");

//		BufferManagerImpl bufferManager = new BufferManagerImpl(100_000);
//		BTreeImpl movieIdBtree = new BTreeImpl(bf, MOVIE_ID_INDEX_PAGE_INDEX);
//		Utilities.createMovieIdIndex(bf, movieIdBtree);
//		Iterator<Rid> res = movieIdBtree.search("tt0001053");
////		for(int i = 0 ; i < 73; i++){
////			IndexPage page = (IndexPage) bf.getPage(i,1);
////			byte[] a = page.getByteArray();
////			bf.unpinPage(page.getPid(), 1);
////		}
//		while (res.hasNext()) {
//			Rid r = res.next();
//			int pageId = r.getPageId();
//			int slotId = r.getSlotId();
//			DataPage page = (DataPage) bf.getPage(pageId);
//			Row row = page.getRow(slotId);
//			System.out.println(row);
//			bf.unpinPage(pageId);
//
//		}
//		ScanOperator<MovieRecord> movieScan = new ScanOperator<>(bf, MOVIES_DATA_PAGE_INDEX);
//
//		List<SelectionPredicate> moviePredicates = List.of(
//				new SelectionPredicate(1, "The Messers. Lumière", Comparator.GREATER_THAN_OR_EQUALS),
//				new SelectionPredicate(1, "The Messers. Lumière at Cards", Comparator.LESS_THAN_OR_EQUALS)
//		);
//
//		SelectionOperator<MovieRecord> movieSelection = new SelectionOperator<>(movieScan, moviePredicates);
//
//		movieSelection.open();
//
//		MovieRecord record;
//		while ((record = movieSelection.next()) != null) {
//			System.out.println(record);
//		}
//
//		movieSelection.close();

		// Step 1: Scan WorkedOn
//		ScanOperator<WorkedOnRecord> workedOnScan = new ScanOperator<>(bf, WORKED_ON_DATA_PAGE_INDEX);
//
//// Step 2: Select category = "director"
//		List<SelectionPredicate> workedOnPredicates = List.of(
//				new SelectionPredicate(2, "director", Comparator.EQUALS)
//		);
//		SelectionOperator<WorkedOnRecord> workedOnSelection = new SelectionOperator<>(workedOnScan, workedOnPredicates);
//
//// Step 3: Project movieId, personId
//		ProjectionOperator<WorkedOnRecord, MoviePersonRecord> workedOnProjection =
//				new ProjectionOperator<>(workedOnSelection, ProjectionType.PROJECTION_ON_WORKED_ON);
//
//// Step 4: Materialize
//		MaterializeOperator<MoviePersonRecord> workedOnMaterialized =
//				new MaterializeOperator<>(workedOnProjection, bf, 5);
//
//// Step 5: Use
//		workedOnMaterialized.open();
//		MoviePersonRecord record;
//		while ((record = workedOnMaterialized.next()) != null) {
////			System.out.println(record);
//		}
//		workedOnMaterialized.close();
//
		// Step 1: Scan Movies
//		ScanOperator<MovieRecord> movieScan = new ScanOperator<>(bufferManager, MOVIES_DATA_PAGE_INDEX);
//
//		// Step 2: Selection on Movies (title between start and end)
//		List<SelectionPredicate> moviePredicates = List.of(
//				new SelectionPredicate(1, "A", Comparator.GREATER_THAN_OR_EQUALS),
//				new SelectionPredicate(1, "B", Comparator.LESS_THAN_OR_EQUALS)
//		);
//		SelectionOperator<MovieRecord> movieSelection = new SelectionOperator<>(movieScan, moviePredicates);
//
//		// Step 3: Scan WorkedOn
//		ScanOperator<WorkedOnRecord> workedOnScan = new ScanOperator<>(bufferManager, WORKED_ON_DATA_PAGE_INDEX);
//
//		// Step 4: Selection on WorkedOn (category == "director")
//		List<SelectionPredicate> workedOnPredicates = List.of(
//				new SelectionPredicate(2, "director", Comparator.EQUALS)
//		);
//		SelectionOperator<WorkedOnRecord> workedOnSelection = new SelectionOperator<>(workedOnScan, workedOnPredicates);
//
//		// Step 5: Projection on WorkedOn (keep movieId, personId)
//		ProjectionOperator<WorkedOnRecord, MoviePersonRecord> workedOnProjection =
//				new ProjectionOperator<>(workedOnSelection, ProjectionType.PROJECTION_ON_WORKED_ON);
//
//		// Step 6: Materialize WorkedOn
//		MaterializeOperator<MoviePersonRecord> workedOnMaterialized =
//				new MaterializeOperator<>(workedOnProjection, bufferManager, MOVIE_PERSON_DATA_PAGE_INDEX);
//
//		// Step 7: BNL Join Movies ⨝ WorkedOn
//		BNLJoinOperator<MovieRecord, MoviePersonRecord, MovieWorkedOnJoinRecord> firstJoin =
//				new BNLJoinOperator<>(
//						movieSelection,                    // left child: Movies after title selection
//						workedOnMaterialized,               // right child: filtered+projected WorkedOn
//						0, 0,                               // join on movieId (attribute 0 both sides)
//						49997,                          // (B - C)/2 pages
//						bufferManager,
//						BNL_MOVIE_WORKED_ON_INDEX           // special id for join pages (-1)
//				);
//
//		// Step 8: Open and Fetch Join Results
//		firstJoin.open();
//		MovieWorkedOnJoinRecord joinRecord;
//		while ((joinRecord = firstJoin.next()) != null) {
//			System.out.println(joinRecord);
//		}
//		firstJoin.close();
//
//
//		System.out.println("fininshed");
//		movieIdBtree.printKeys();

//		BTreeImpl movieTitleBtree = new BTreeImpl(bf, MOVIE_TITLE_INDEX_INDEX);
//
////		((BufferManagerImpl) bf).printList();
////		for(int i = 0; i < 15; i++) {
////			DataPage page = (DataPage) bf.createPage();
////			int pageId = page.getPid();
////			System.out.println(i);
////		    ((BufferManagerImpl) bf).printList();
////			Row row1 = new Row(("tt00000" + i).getBytes(), ("Movie " + i).getBytes());
////			page.insertRow(row1);
////			// Unpin the page
////			bf.unpinPage(pageId);
////		}
////		bf.force();
////		DataPage page = (DataPage) bf.getPage(14);
////		int i = 0;
////		while(true){
////			Row row = page.getRow(i);
////			if( row == null){
////				System.out.println(i);
////				break;
////			}
////			System.out.println(row);
////			i++;
////		}
//		Utilities.createMovieTitleIndex(bf,movieTitleBtree);
//		Iterator<Rid> rd = movieTitleBtree.search("Movie 10");
//

		if (args.length != 3) {
			System.err.println("Usage: run_query <start_range> <end_range> <buffer_size>");
			System.exit(1);
		}

		String startRange = args[0];
		String endRange = args[1];
		int bufferSize = Integer.parseInt(args[2]);

		// Initialize Buffer Manager
		BufferManagerImpl bufferManager = new BufferManagerImpl(bufferSize);

		// Scan Movies
		ScanOperator<MovieRecord> movieScan = new ScanOperator<>(bufferManager, MOVIES_DATA_PAGE_INDEX);

		// Select Movies title BETWEEN startRange and endRange
		List<SelectionPredicate> moviePredicates = List.of(
				new SelectionPredicate(1, startRange, Comparator.GREATER_THAN_OR_EQUALS),
				new SelectionPredicate(1, endRange, Comparator.LESS_THAN_OR_EQUALS)
		);
		SelectionOperator<MovieRecord> movieSelection = new SelectionOperator<>(movieScan, moviePredicates);

		// Scan WorkedOn
		ScanOperator<WorkedOnRecord> workedOnScan = new ScanOperator<>(bufferManager, WORKED_ON_DATA_PAGE_INDEX);

		// Select WorkedOn where category = director
		List<SelectionPredicate> workedOnPredicates = List.of(
				new SelectionPredicate(2, "director", Comparator.EQUALS)
		);
		SelectionOperator<WorkedOnRecord> workedOnSelection = new SelectionOperator<>(workedOnScan, workedOnPredicates);

		// Projection on WorkedOn to (movieId, personId)
		ProjectionOperator<WorkedOnRecord, MoviePersonRecord> workedOnProjection =
				new ProjectionOperator<>(workedOnSelection, ProjectionType.PROJECTION_ON_WORKED_ON);

		// Materialize WorkedOn
		MaterializeOperator<MoviePersonRecord> workedOnMaterialized =
				new MaterializeOperator<>(workedOnProjection, bufferManager, MOVIE_PERSON_DATA_PAGE_INDEX);

		// First BNL Join: Movies ⋈ WorkedOn
		int blockSize = (bufferSize - 4) / 2; // assume C = 4 for safety
		BNLJoinOperator<MovieRecord, MoviePersonRecord, MovieWorkedOnJoinRecord> firstJoin =
				new BNLJoinOperator<>(
						movieSelection,
						workedOnMaterialized,
						0, 0,
						blockSize,
						bufferManager,
						BNL_MOVIE_WORKED_ON_INDEX
				);

		// Scan People
		ScanOperator<PeopleRecord> peopleScan = new ScanOperator<>(bufferManager, PEOPLE_DATA_PAGE_INDEX);

		// Second BNL Join: (Movies ⋈ WorkedOn) ⋈ People
		BNLJoinOperator<MovieWorkedOnJoinRecord, PeopleRecord, MovieWorkedOnPeopleJoinRecord> secondJoin =
				new BNLJoinOperator<>(
						firstJoin,
						peopleScan,
						1, 0, // personId from left join, personId from People
						blockSize,
						bufferManager,
						BNL_MOVIE_WORKED_ON_PEOPLE_INDEX
				);

		// Final Projection: (title, name)
		ProjectionOperator<MovieWorkedOnPeopleJoinRecord, TitleNameRecord> finalProjection =
				new ProjectionOperator<>(secondJoin, ProjectionType.PROJECTION_ON_FINAL_JOIN);

		// Open the pipeline
		finalProjection.open();

		// Prepare Excel file
		Workbook workbook = new XSSFWorkbook();
		Sheet sheet = workbook.createSheet("QueryResults");
		int rowNum = 0;
		Row headerRow = sheet.createRow(rowNum++);
		headerRow.createCell(0).setCellValue("Title");
		headerRow.createCell(1).setCellValue("Name");

		TitleNameRecord output;
		while ((output = finalProjection.next()) != null) {
			String title = new String(output.movieTitle()).trim();
			String name = new String(output.personName()).trim();

			// Print to console
			System.out.println(title + "," + name);

			// Write to Excel
			Row row = sheet.createRow(rowNum++);
			row.createCell(0).setCellValue(title);
			row.createCell(1).setCellValue(name);
		}

		finalProjection.close();

		// Write Excel file
		try (FileOutputStream fileOut = new FileOutputStream("query_output.xlsx")) {
			workbook.write(fileOut);
			workbook.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		System.out.println("Query completed. Results saved to query_output.xlsx");
	}

}
