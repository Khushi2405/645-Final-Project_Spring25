package com.database.finalproject.repository;

import com.database.finalproject.model.Page;

import java.util.HashMap;
import java.util.Map;

public class PageRepository {

    private final Map<Integer, Page> pageStorage = new HashMap<>();

    public Page getPage(int pageId) {
        return pageStorage.get(pageId);
    }

    public void savePage(int pageId, Page page) {
        pageStorage.put(pageId, page);
    }

    public void deletePage(int pageId) {
        pageStorage.remove(pageId);
    }
}
