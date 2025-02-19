package com.database.finalproject;

import com.database.finalproject.repository.Utilities;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


import java.io.IOException;

@SpringBootApplication
public class DatabaseFinalProjectApplication {

	public static void main(String[] args) throws IOException {
		SpringApplication.run(DatabaseFinalProjectApplication.class, args);
		Utilities.loadDataset("src/main/resources/static/title.basics.tsv");
		Utilities.fetchPage(1);
	}

}
