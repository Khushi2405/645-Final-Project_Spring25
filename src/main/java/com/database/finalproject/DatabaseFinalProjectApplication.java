package com.database.finalproject;

import com.database.finalproject.btree.BTreeImpl;
import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.buffermanager.BufferManagerImpl;
import com.database.finalproject.controller.UserController;
import com.database.finalproject.model.*;
import com.database.finalproject.repository.Utilities;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Iterator;

import static com.database.finalproject.constants.PageConstants.MOVIE_ID_INDEX_PAGE_INDEX;
import static com.database.finalproject.constants.PageConstants.MOVIE_TITLE_INDEX_INDEX;

@SpringBootApplication
public class DatabaseFinalProjectApplication {

	public static void main(String[] args) {
		SpringApplication.run(DatabaseFinalProjectApplication.class, args);
//		UserController controller = new UserController(100);
//		controller.searchMovieId("Episode dated 2 February 1990");

		BufferManager bf = new BufferManagerImpl(10);
		BTreeImpl movieIdBtree = new BTreeImpl(bf, MOVIE_ID_INDEX_PAGE_INDEX);
		Utilities.createMovieIdIndex(bf, movieIdBtree);
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
		System.out.println("fininshed");
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

	}

}
