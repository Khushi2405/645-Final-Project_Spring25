package com.database.finalproject;

import com.database.finalproject.controller.UserController;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;



@SpringBootApplication
public class DatabaseFinalProjectApplication {

	public static void main(String[] args) {
		SpringApplication.run(DatabaseFinalProjectApplication.class, args);
//
		if(args.length == 4 && args[0].equals("run_query")){
			String startRange = args[1];
			String endRange = args[2];
			int bufferSize = Integer.parseInt(args[3]);
			UserController controller = new UserController(bufferSize);
			controller.runQuery(startRange, endRange);
		}
	}

}
