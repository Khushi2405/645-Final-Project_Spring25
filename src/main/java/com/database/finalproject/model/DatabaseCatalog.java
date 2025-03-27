package com.database.finalproject.model;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabaseCatalog {
    private List<Map<String, String>> catalog;
    String catalogFile;

    public DatabaseCatalog(String catalogFile) {
        this.catalog = new ArrayList<>();
        this.catalogFile = catalogFile;
        loadCatalog(catalogFile);
    }

    private void loadCatalog(String catalogFile) {
        try (BufferedReader reader = new BufferedReader(new FileReader(catalogFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                Map<String, String> entry = new HashMap<>();
                entry.put("filename", parts[0]);
                entry.put("totalPages", parts[1]);
                entry.put("rootPage", parts[2]);
                catalog.add(entry);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map<String, String> getCatalog(int index) {
        if(index == catalog.size()) return null;
        return catalog.get(index);
    }

    public void setCatalog(int index, String key, String value){
        if(index == catalog.size()) return;
        catalog.get(index).put(key, value);
        saveCatalog();
    }

    public void saveCatalog() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(catalogFile))) {
            for (Map<String, String> entry : catalog) {
                writer.write(entry.get("filename") + "," + entry.get("totalPages") + "," + entry.get("rootPage") + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}