package com.database.finalproject.buffermanager;

import com.database.finalproject.model.DataPage;
import com.database.finalproject.model.Row;
import com.database.finalproject.repository.Utilities;
import org.junit.jupiter.api.Test;

import static com.database.finalproject.constants.PageConstants.PADDING_BYTE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class ClosedTests {


        //     Test1:
//     For a buffer size of 1, Continuosly insert, evict and check
//     Evicted page must be written to disk.
        @Test
        void testCreationAndEviction(){
            // Create a buffer of size 2 pages
            int bufferSize = 1;
            BufferManagerImpl bf = new BufferManagerImpl(bufferSize);
            int counter = 0;
            int maxPages = 1000;
            int rows_per_page = 0;
            for(int i = 0; i < maxPages; i ++){
                DataPage page = (DataPage) bf.createPage();
                while(! page.isFull()){
                    String movie =  "movie" + counter + "****";
                    movie = movie.substring(0, 9);
                    String title = "title" + counter;
                    Row row =new Row(movie.getBytes(), title.getBytes());
                    page.insertRow(row);
                    counter = counter + 1;
                }
                if (i == 0) rows_per_page =counter;
                bf.markDirty(i);
                bf.unpinPage(i);
            }
            int maxRows = counter;
            // System.out.print("max rows" +  maxRows + " " + rows_per_page);
            counter = 0;
            for(int i = 0; i < maxPages; i ++){
                DataPage page = (DataPage) bf.getPage(i);
                for(int localRow = 0; localRow < rows_per_page; localRow ++ ){
                    if(page == null){
                        System.out.println();
                    }
                    Row row = page.getRow(localRow);
                    String movString = "movie" + counter + "****";
                    if (movString.length()>9){
                        movString = movString.substring(0, 9);
                    }
                    String refmovie =  movString;
                    String reftitle = "title" + counter;
                    assertEquals(new String(row.movieId()), refmovie);
                    assertEquals(new String(removeTrailingBytes(row.movieTitle())), reftitle);
                    counter = counter + 1;
                }
                bf.unpinPage(i);
            }
        }


        // Updates not marked dirty are not persisted.
        @Test
        void testMarkDirty(){
            // Create a buffer of size 2 pages
            int bufferSize = 2;
            BufferManagerImpl bf = new BufferManagerImpl(bufferSize);
            int counter = 0;
            int maxPages = 4;
            int rows_per_page = 0;

            for(int i = 0; i < maxPages; i ++){
                DataPage page = (DataPage) bf.createPage();
                bf.unpinPage(i);
            }
            for(int i = 0; i < maxPages; i ++){
                DataPage page = (DataPage) bf.getPage(i);
                while(! page.isFull()){
                    String movie =  "movie" + counter + "****";
                    movie = movie.substring(0, 9);
                    String title = "title" + counter;
                    Row row =new Row(movie.getBytes(), title.getBytes());
                    page.insertRow(row);
                    counter = counter + 1;
                }
                if (i == 0) rows_per_page =counter;
                if(i %2 == 0) bf.markDirty(i);
                bf.unpinPage(i);
            }

            for(int i = 0; i < maxPages; i ++ ){
                DataPage pg = (DataPage) bf.getPage(i);
                if (i % 2 == 0){
                    assertTrue(pg.isFull());
                }else{
                    assertFalse(pg.isFull());
                }
                bf.unpinPage(pg.getPid());
            }
        }
//
        // LRU eviction of pages.
        @Test
        void testLRUEviction(){
            // Create a buffer of size 2 pages
            int bufferSize = 5;
            BufferManagerImpl bf = new BufferManagerImpl(bufferSize);
            int counter = 0;
            int maxPages = 1000;
            int rows_per_page = 0;
            for(int i = 0; i < maxPages; i ++){
                DataPage page = (DataPage) bf.createPage();
                while(! page.isFull()){
                    String movie =  "movie" + counter + "****";
                    movie = movie.substring(0, 9);
                    String title = "title" + counter;
                    Row row =new Row(movie.getBytes(), title.getBytes());
                    page.insertRow(row);
                    counter = counter + 1;
                }
                if (i == 0) rows_per_page =counter;
                bf.unpinPage(i);
                bf.getPage(0);
                bf.unpinPage(0);

            }
            long s1 = System.nanoTime();
            bf.getPage(0);
            long e1 = System.nanoTime();
            long d1 = e1 - s1;

            long s2 = System.nanoTime();
            bf.getPage(maxPages - 1);
            long e2 = System.nanoTime();
            long d2 = e2 - s2;


            long s3 = System.nanoTime();
            bf.getPage(1);
            long e3 = System.nanoTime();
            long d3 = e3 - s3;
            System.out.println("" + d3 + " " + d1 + " " + d2);
            assertTrue(d3 > d1 * 2);
            assertTrue(d3 > d2 * 2);
        }
//
//        // pinned pages are not evicted, creating more than buffer manager size causes exception
        @Test
        void testPinnedEviction(){
            // Create a buffer of size 2 pages
            int bufferSize = 3;
            BufferManagerImpl bf = new BufferManagerImpl(bufferSize);
            int counter = 0;
            int maxPages = 10;
            int rows_per_page = 0;
            try{
                for(int i = 0; i < maxPages; i ++){
                    DataPage page = (DataPage) bf.createPage();
                    while(! page.isFull()){
                        String movie =  "movie" + counter + "****";
                        movie = movie.substring(0, 9);
                        String title = "title" + counter;
                        Row row =new Row(movie.getBytes(), title.getBytes());
                        page.insertRow(row);
                        counter = counter + 1;
                    }

                }
                assertTrue(false);
            }catch (Exception e){
                assertTrue(true);
            }
        }

        // test loading of imdb dataset
//        @Test
//        void testImdbDataset(){
//            // Create a buffer of size 2 pages
//            int bufferSize1 = 3;
//            BufferManagerImpl bf1 = new BufferManagerImpl(bufferSize1);
//
//            String filepath = "title.basics.tsv";
//            Utilities.loadDataset(bf1, filepath);
//            Row row1 = bf1.getPage(0).getRow(19);
//
//            int bufferSize2 = 30;
//            BufferImp bf2 = new BufferImp(bufferSize2);
//            ut.loadDataset(bf2, filepath);
//            Row row2 = bf1.getPage(0).getRow(19);
//
//            assertEquals(new String(row1.movieId),new String(row2.movieId));
//        }

    private static byte[] truncateOrPadByteArray(byte[] value, int maxLength) {
        if (value.length > maxLength) {
            return Arrays.copyOf(value, maxLength); // Truncate safely at byte level
        } else {
            byte[] padded = new byte[maxLength];
            System.arraycopy(value, 0, padded, 0, value.length); // Copy original bytes
            Arrays.fill(padded, value.length, maxLength, PADDING_BYTE); // Fill remaining space with 0x7F
            return padded;
        }
    }

    private static byte[] removeTrailingBytes(byte[] input) {
        int endIndex = input.length;
        for (int i = input.length - 1; i >= 0; i--) {
            if (input[i] != PADDING_BYTE) { // Only remove custom padding byte
                endIndex = i + 1;
                break;
            }
        }
        return Arrays.copyOf(input, endIndex);
    }
}
