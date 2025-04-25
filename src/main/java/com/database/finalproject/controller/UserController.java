package com.database.finalproject.controller;

import com.database.finalproject.btree.BTreeImpl;
import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.buffermanager.BufferManagerImpl;
import com.database.finalproject.model.page.MovieDataPage;
import com.database.finalproject.model.record.MovieRecord;
import com.database.finalproject.model.Rid;
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
//        this.movieIdBtree = new BTreeImpl(bf, MOVIE_ID_INDEX_PAGE_INDEX);
//        this.movieTitleBtree = new BTreeImpl(bf, MOVIE_TITLE_INDEX_INDEX);
//        Utilities.loadMoviesDataset(bf, MOVIE_DATABASE_FILE);
//        Utilities.loadWorkedOnDataset(bf, WORKED_ON_DATABASE_FILE);
//        Utilities.loadPeopleDataset(bf, PEOPLE_DATABASE_FILE);
//        Utilities.createMovieIdIndex(bf, movieIdBtree);
//        Utilities.createMovieTitleIndex(bf, movieTitleBtree);
    }

    public MovieDataPage createMoviePage(){
        return (MovieDataPage) bf.createPage();
    }

    public MovieDataPage getMoviePage(int pageId){
        return (MovieDataPage) bf.getPage(pageId);
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

    public List<MovieRecord> searchMovieId(String key){
        Iterator<Rid> res = movieIdBtree.search(key);
        return fetchRows(res);
    }

    public List<MovieRecord> searchMovieTitle(String key){
        Iterator<Rid> res = movieTitleBtree.search(key);
        return fetchRows(res);
    }

    public List<MovieRecord> rangeSearchMovieId(String startKey, String endKey){
        Iterator<Rid> res = movieIdBtree.rangeSearch(startKey, endKey);
        return fetchRows(res);
    }

    public List<MovieRecord> rangeSearchMovieTitle(String startKey, String endKey){
        Iterator<Rid> res = movieTitleBtree.rangeSearch(startKey, endKey);
        return fetchRows(res);
    }

    private List<MovieRecord> fetchRows(Iterator<Rid> res) {
        List<MovieRecord> ans = new ArrayList<>();
        while (res.hasNext()) {
            Rid r = res.next();
            int pageId = r.getPageId();
            int slotId = r.getSlotId();
            MovieDataPage page = (MovieDataPage) bf.getPage(pageId);
            MovieRecord movieRecord = page.getRecord(slotId);
            ans.add(movieRecord);
            bf.unpinPage(pageId);


        }
        return ans;
    }


}
