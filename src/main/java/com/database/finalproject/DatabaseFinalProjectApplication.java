package com.database.finalproject;

import com.database.finalproject.controller.UserController;
import com.database.finalproject.repository.Utilities;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.*;

import static com.database.finalproject.constants.PageConstants.removeTrailingBytes;
import static com.database.finalproject.constants.PageConstants.truncateOrPadByteArray;


@SpringBootApplication
public class DatabaseFinalProjectApplication {

	public static void main(String[] args) {
//		SpringApplication.run(DatabaseFinalProjectApplication.class, args);
////
//		if(args.length == 4 && args[0].equals("run_query")){
//			String startRange = args[1];
//			String endRange = args[2];
//			int bufferSize = Integer.parseInt(args[3]);
//			UserController controller = new UserController(bufferSize);
//			controller.runQuery(startRange, endRange);
//		}



		Utilities.convertMovies("src/main/resources/static/title.basics.tsv", "src/main/resources/static/ActualMovies.csv");

		// WorkedOn
		Utilities.convertWorkedOn("src/main/resources/static/title.principals.tsv", "src/main/resources/static/ActualWorkedOn.csv");

		// People
		Utilities.convertPeople("src/main/resources/static/name.basics.tsv", "src/main/resources/static/ActualPeople.csv");

	}



}
