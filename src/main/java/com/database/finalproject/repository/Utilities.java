package com.database.finalproject.repository;

import com.database.finalproject.btree.BTree;
import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.model.page.MovieDataPage;
import com.database.finalproject.model.page.PeopleDataPage;
import com.database.finalproject.model.page.WorkedOnDataPage;
import com.database.finalproject.model.record.MovieRecord;
import com.database.finalproject.model.page.Page;
import com.database.finalproject.model.Rid;
import com.database.finalproject.model.record.PeopleRecord;
import com.database.finalproject.model.record.WorkedOnRecord;

import java.io.*;
import java.util.Arrays;

import static com.database.finalproject.constants.PageConstants.*;


public class Utilities {

    public static void loadMoviesDataset(BufferManager bf, String filepath) {
        System.out.println("Opening Movies Dataset file");
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            System.out.println("File opened, loading dataset to file");
            MovieDataPage currPage = (MovieDataPage) bf.createPage(MOVIES_DATA_PAGE_INDEX);
            int pageId = currPage.getPid();

            // Skip header line
            String line = br.readLine();

            // Process each line from the input file
            while ((line = br.readLine()) != null) {
                String[] columns = line.split("\t");
                if (columns.length < 3)
                    continue; // Skip invalid rows
                byte[] movieId = columns[0].getBytes(); // tconst
                byte[] movieTitle = columns[2].getBytes(); // primaryTitle
                if (movieId.length > 9) {
                    continue;
                }

                MovieRecord movieRecord = new MovieRecord(movieId, movieTitle);
                currPage.insertRecord(movieRecord);
                if (currPage.isFull()) {
                    bf.unpinPage(pageId, MOVIES_DATA_PAGE_INDEX);
                    currPage = (MovieDataPage) bf.createPage(MOVIES_DATA_PAGE_INDEX);
                    pageId = currPage.getPid();
                }
            }
            bf.unpinPage(pageId, MOVIES_DATA_PAGE_INDEX);
            bf.force();
            System.out.println("Dataset loaded");
            System.out.println("Last Page ID: " + currPage.getPid());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void loadPeopleDataset(BufferManager bf, String filepath) {
        System.out.println("Opening people dataset file");
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            System.out.println("File opened, loading dataset to file");
            PeopleDataPage currPage = (PeopleDataPage) bf.createPage(PEOPLE_DATA_PAGE_INDEX);
            int pageId = currPage.getPid();

            // Skip header line
            String line = br.readLine();

            // Process each line from the input file
            while ((line = br.readLine()) != null) {
                String[] columns = line.split("\t");
                if (columns.length < 2)
                    continue; // Skip invalid rows
                byte[] personId = columns[0].getBytes(); // nconst
                byte[] name = columns[1].getBytes();     // primaryName
                if (personId.length > 10) {
                    continue;
                }

                PeopleRecord peopleRecord = new PeopleRecord(personId, name);
                currPage.insertRecord(peopleRecord);
                if (currPage.isFull()) {
                    if(currPage.getPid()%10000 == 0){
                        System.out.println(currPage.getPid() + " loaded");
                    }
                    bf.unpinPage(pageId, PEOPLE_DATA_PAGE_INDEX);
                    currPage = (PeopleDataPage) bf.createPage(PEOPLE_DATA_PAGE_INDEX);
                    pageId = currPage.getPid();
                }
            }
            bf.unpinPage(pageId, PEOPLE_DATA_PAGE_INDEX);
            bf.force();
            System.out.println("Dataset loaded");
            System.out.println("Last Page ID: " + currPage.getPid());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void loadWorkedOnDataset(BufferManager bf, String filepath) {
        System.out.println("Opening worked on dataset file");
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            System.out.println("File opened, loading dataset to file");
            WorkedOnDataPage currPage = (WorkedOnDataPage) bf.createPage(WORKED_ON_DATA_PAGE_INDEX);
            int pageId = currPage.getPid();

            // Skip header line
            String line = br.readLine();

            // Process each line from the input file
            while ((line = br.readLine()) != null) {
                String[] columns = line.split("\t");
                if (columns.length < 4)
                    continue; // Skip invalid rows
                byte[] movieId = columns[0].getBytes(); // tconst
                byte[] personId = columns[2].getBytes(); // nconst
                byte[] category = columns[3].getBytes(); // category
                if (movieId.length > 9 || personId.length > 10) {
                    continue;
                }

                WorkedOnRecord workedOnRecord = new WorkedOnRecord(movieId, personId, category);
                currPage.insertRecord(workedOnRecord);
                if (currPage.isFull()) {
                    bf.unpinPage(pageId, WORKED_ON_DATA_PAGE_INDEX);
                    if(currPage.getPid()%10000 == 0){
                        System.out.println(currPage.getPid() + " loaded");
                    }
                    currPage = (WorkedOnDataPage) bf.createPage(WORKED_ON_DATA_PAGE_INDEX);
                    pageId = currPage.getPid();
                }
            }
            System.out.println("Page loaded: " + pageId );
            bf.unpinPage(pageId, WORKED_ON_DATA_PAGE_INDEX);
            bf.force();
            System.out.println("Dataset loaded");
            System.out.println("Last Page ID: " + currPage.getPid());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createMovieIdIndex(BufferManager bf, BTree<String, Rid> b) {
        int dataPageId = 0;
        while (true) {
            Page currPage = bf.getPage(dataPageId, MOVIES_DATA_PAGE_INDEX);
            if (currPage == null)
                break;
            for (int i = 0; i < 105; i++) {
                MovieRecord movieRecord = ((MovieDataPage) currPage).getRecord(i);
                if (movieRecord == null)
                    break;
                b.insert(new String(movieRecord.movieId()).trim(), new Rid(dataPageId, i));
            }
            bf.unpinPage(dataPageId, MOVIES_DATA_PAGE_INDEX);
            if(dataPageId % 1000 == 0) System.out.println(dataPageId + " inserted");
            dataPageId++;
        }
        bf.force();
    }

    public static void createMovieTitleIndex(BufferManager bf, BTree<String, Rid> b) {
        int dataPageId = 0;
        while (true) {
            Page currPage = bf.getPage(dataPageId, MOVIES_DATA_PAGE_INDEX);
            if (currPage == null)
                break;
            for (int i = 0; i < 105; i++) {
                MovieRecord movieRecord = ((MovieDataPage) currPage).getRecord(i);
                if (movieRecord == null)
                    break;
                b.insert(new String(removeTrailingBytes(movieRecord.movieTitle())).trim(), new Rid(dataPageId, i));
            }
            bf.unpinPage(dataPageId, MOVIES_DATA_PAGE_INDEX);
            if(dataPageId % 1000 == 0) System.out.println(dataPageId + " inserted");
            dataPageId++;
        }
        bf.force();
    }

    public static void convertMovies(String inputFile, String outputFile) {
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile));
             BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {

            // Skip header
            String line = br.readLine();

            while ((line = br.readLine()) != null) {
                String[] columns = line.split("\t");
                if (columns.length < 3) continue;

                byte[] movieId = columns[0].getBytes();
                byte[] movieTitle = columns[2].getBytes();

                if (movieId.length > 9) continue;

                // Truncate to expected sizes
                movieId = truncateOrPadByteArray(movieId, 9);
                movieTitle = truncateOrPadByteArray(movieTitle, 30);

                String movieIdStr = new String(removeTrailingBytes(movieId)).trim();
                String movieTitleStr = new String(removeTrailingBytes(movieTitle)).trim();

                bw.write(movieIdStr + "," + movieTitleStr);
                bw.newLine();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        countRows(outputFile, "Movies");
    }

    public static void convertPeople(String inputFile, String outputFile) {
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile));
             BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {

            // Skip header
            String line = br.readLine();

            while ((line = br.readLine()) != null) {
                String[] columns = line.split("\t");
                if (columns.length < 2) continue;

                byte[] personId = columns[0].getBytes();
                byte[] name = columns[1].getBytes();

                if (personId.length > 10) continue;

                personId = truncateOrPadByteArray(personId, 10);
                name = truncateOrPadByteArray(name, 105);

                String personIdStr = new String(removeTrailingBytes(personId)).trim();
                String nameStr = new String(removeTrailingBytes(name)).trim();

                bw.write(personIdStr + "," + nameStr);
                bw.newLine();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        countRows(outputFile, "People");
    }

    public static void convertWorkedOn(String inputFile, String outputFile) {
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile));
             BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {

            // Skip header
            String line = br.readLine();

            while ((line = br.readLine()) != null) {
                String[] columns = line.split("\t");
                if (columns.length < 4) continue;

                byte[] movieId = columns[0].getBytes();
                byte[] personId = columns[2].getBytes();
                byte[] category = columns[3].getBytes();

                if (movieId.length > 9 || personId.length > 10) continue;

                movieId = truncateOrPadByteArray(movieId, 9);
                personId = truncateOrPadByteArray(personId, 10);
                category = truncateOrPadByteArray(category, 20);

                String movieIdStr = new String(removeTrailingBytes(movieId)).trim();
                String personIdStr = new String(removeTrailingBytes(personId)).trim();
                String categoryStr = new String(removeTrailingBytes(category)).trim();

                bw.write(movieIdStr + "," + personIdStr + "," + categoryStr);
                bw.newLine();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        countRows(outputFile, "WorkedOn");
    }

    private static void countRows(String filePath, String datasetName) {
        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            while (br.readLine() != null) {
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(datasetName + ": " + count + " rows confirmed in " + filePath);
    }
}
