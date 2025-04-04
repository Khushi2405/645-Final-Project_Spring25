package com.database.finalproject.model;

import static com.database.finalproject.constants.PageConstants.DATABASE_CATALOGUE_KEY_FILENAME;
import static com.database.finalproject.constants.PageConstants.DATABASE_CATALOGUE_KEY_ROOT_PAGE;
import static com.database.finalproject.constants.PageConstants.DATABASE_CATALOGUE_KEY_TOTAL_PAGES;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabaseCatalog {
    private List<Map<String, String>> catalog;
    String catalogFile;
    RandomAccessFile raf;

    public DatabaseCatalog(String catalogFile) {
        this.catalog = new ArrayList<>();
        this.catalogFile = catalogFile;
        try {
            this.raf = new RandomAccessFile(catalogFile, "rwd");
        } catch (Exception e) {
            // TODO: handle exception
            System.out.println("Error in RAF, file cannot be created");
            throw new RuntimeException(e);
        }
        loadCatalog(catalogFile);
    }

    private void loadCatalog(String catalogFile) {
        try (RandomAccessFile raf = new RandomAccessFile(catalogFile, "r")) {
            String line;
            while ((line = raf.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 3) { // Ensure correct splitting
                    Map<String, String> entry = new HashMap<>();
                    entry.put(DATABASE_CATALOGUE_KEY_FILENAME, parts[0].trim());
                    entry.put(DATABASE_CATALOGUE_KEY_TOTAL_PAGES, parts[1].trim());
                    entry.put(DATABASE_CATALOGUE_KEY_ROOT_PAGE, parts[2].trim());
                    catalog.add(entry);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map<String, String> getCatalog(int index) {
        if (index == catalog.size())
            return null;
        return catalog.get(index);
    }

    public void setCatalog(int index, String key, String value) {
        if (index == catalog.size())
            return;
        catalog.get(index).put(key, value);
        saveCatalog();
    }

    public void saveCatalog() {
        try {
            this.raf.setLength(0);
            for (Map<String, String> entry : catalog) {
                this.raf.writeBytes(
                        entry.get(DATABASE_CATALOGUE_KEY_FILENAME) + "," +
                                entry.get(DATABASE_CATALOGUE_KEY_TOTAL_PAGES)
                                + "," + entry.get(DATABASE_CATALOGUE_KEY_ROOT_PAGE) + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}