package com.database.finalproject;

import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.buffermanager.BufferManagerImpl;
import com.database.finalproject.model.Page;
import com.database.finalproject.model.PageImpl;
import com.database.finalproject.model.Row;
import com.database.finalproject.repository.Utilities;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static com.database.finalproject.constants.PageConstants.PAGE_SIZE;

@SpringBootApplication
public class DatabaseFinalProjectApplication {

	public static void main(String[] args) throws IOException {
		SpringApplication.run(DatabaseFinalProjectApplication.class, args);
		BufferManager bufferManager = new BufferManagerImpl(10);
		//Utilities.loadDataset(bufferManager, "src/main/resources/static/title.basics.tsv");
		//((BufferManagerImpl) bufferManager).readPage(1);
//		Page page = bufferManager.createPage();
//		int pageId = page.getPid();
//		System.out.println("print in main " + pageId);
//		String id = "1234";
//		String title = "atqf";
//		byte[] movieId = id.getBytes(StandardCharsets.UTF_8);
//		byte[] movieTitle = title.getBytes(StandardCharsets.UTF_8);
//		Row row = new Row(movieId, movieTitle);
//		page.insertRow(row);
//		((BufferManagerImpl) bufferManager).writeToBinaryFile(page);
		//((BufferManagerImpl) bufferManager).readPage(2);
		//System.out.println("written " + pageId);
//		Page page = bufferManager.getPage(2);
//		String id = "12345678";
//		String title = "atqfupdated";
//		byte[] movieId = id.getBytes(StandardCharsets.UTF_8);
//		byte[] movieTitle = title.getBytes(StandardCharsets.UTF_8);
//		Row row = new Row(movieId, movieTitle);
//		page.insertRow(row);
//		String id1 = "123456780";
//		String title1 = "try updating middle";
//		byte[] movieId1 = id1.getBytes(StandardCharsets.UTF_8);
//		byte[] movieTitle1 = title1.getBytes(StandardCharsets.UTF_8);
//		Row row1 = new Row(movieId1, movieTitle1);
//		page.insertRow(row1);
//		((BufferManagerImpl) bufferManager).writeToBinaryFile(page);
//		//System.out.println(((PageImpl)page).nextRowId  + " try " + page.getPid());
//		((BufferManagerImpl) bufferManager).readPage(2);
//		((BufferManagerImpl) bufferManager).readPage(3);

//		Page page2 = bufferManager.createPage();
//
//		String id = "1234new";
//		String title = "new page ";
//		byte[] movieId = id.getBytes(StandardCharsets.UTF_8);
//		byte[] movieTitle = title.getBytes(StandardCharsets.UTF_8);
//		Row row = new Row(movieId, movieTitle);
//		page2.insertRow(row);
//		((BufferManagerImpl) bufferManager).writeToBinaryFile(page2);
//		((BufferManagerImpl) bufferManager).readPage(3);

//		File file = new File("src/main/resources/static/binary_heap.bin");
//
//		if (file.exists()) {
//			long fileSize = file.length(); // Size in bytes
//			System.out.println("File size: " + fileSize/PAGE_SIZE + " bytes");
//			//maxPages = (int)fileSize/PAGE_SIZE;
//		} else {
//			System.out.println("File does not exist.");
//		}

		for(int i = 1 ; i < 12; i++){
			bufferManager.getPage(i);
			bufferManager.unpinPage(i);
		}

		bufferManager.getPage(2);
		bufferManager.getPage(1);
		Page page2 = bufferManager.createPage();
		String id = "1234new";
		String title = "new page ";
		byte[] movieId = id.getBytes(StandardCharsets.UTF_8);
		byte[] movieTitle = title.getBytes(StandardCharsets.UTF_8);
		Row row = new Row(movieId, movieTitle);
		page2.insertRow(row);
		int pageId = page2.getPid();
		bufferManager.markDirty(pageId);
		System.out.println(pageId);
		bufferManager.unpinPage(pageId);
		for(int i = 1 ; i < 12; i++){
			bufferManager.getPage(i);
			bufferManager.unpinPage(i);
		}
		((BufferManagerImpl) bufferManager).readPage(pageId);








	}

}
