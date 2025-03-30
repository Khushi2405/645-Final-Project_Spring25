package com.database.finalproject.controller;

import com.database.finalproject.btree.BTree;
import com.database.finalproject.btree.BTreeImpl;
import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.buffermanager.BufferManagerImpl;
import com.database.finalproject.model.DataPage;
import com.database.finalproject.model.Page;
import com.database.finalproject.model.Rid;
import com.database.finalproject.model.Row;
import com.database.finalproject.repository.Utilities;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.database.finalproject.constants.PageConstants.*;

public class UserController {
    BufferManager bf;
    BTreeImpl movieIdBtree;
    BTreeImpl movieTitleBtree;

    public UserController(int bufferSize) {
        this.bf = new BufferManagerImpl(bufferSize);
        this.movieIdBtree = new BTreeImpl(bf, MOVIE_ID_INDEX_PAGE_INDEX);
        this.movieTitleBtree = new BTreeImpl(bf, MOVIE_TITLE_INDEX_INDEX);
        Utilities.loadDataset(bf, DATABASE_FILE);
        Utilities.createMovieIdIndex(bf, movieIdBtree);
        Utilities.createMovieTitleIndex(bf, movieTitleBtree);
    }

    public DataPage createPage(){
        return (DataPage) bf.createPage();
    }

    public DataPage getPage(int pageId){
        return (DataPage) bf.getPage(pageId);
    }

    public void makeDirty(int pageId){
        bf.markDirty(pageId);
    }

    public void unpinPage(int pageId){
        bf.unpinPage(pageId);
    }

    public void force(){
        bf.force();
    }

    public List<Row> searchMovieId(String key){
        Iterator<Rid> res = movieIdBtree.search(key);
        return fetchRows(res);
    }

    public List<Row> searchMovieTitle(String key){
        Iterator<Rid> res = movieTitleBtree.search(key);
        return fetchRows(res);
    }

    public List<Row> rangeSearchMovieId(String startKey, String endKey){
        Iterator<Rid> res = movieIdBtree.rangeSearch(startKey, endKey);
        return fetchRows(res);
    }

    public List<Row> rangeSearchMovieTitle(String startKey, String endKey){
        Iterator<Rid> res = movieTitleBtree.rangeSearch(startKey, endKey);
        return fetchRows(res);
    }

    private List<Row> fetchRows(Iterator<Rid> res) {
        List<Row> ans = new ArrayList<>();
        while (res.hasNext()) {
            Rid r = res.next();
            int pageId = r.getPageId();
            int slotId = r.getSlotId();
            DataPage page = (DataPage) bf.getPage(pageId);
            Row row = page.getRow(slotId);
            ans.add(row);
            bf.unpinPage(pageId);


        }
        return ans;
    }


}
