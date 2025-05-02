package com.database.finalproject.queryplan;

// ScanOperator.java
import com.database.finalproject.buffermanager.BufferManagerImpl;
import com.database.finalproject.model.page.DataPage;
import com.database.finalproject.model.page.Page;
import com.database.finalproject.model.record.ParentRecord;

public class ScanOperator<T extends ParentRecord> implements Operator<T> {
    private final BufferManagerImpl bufferManager;
    private final int catalogIndex;
    private int currentPageId;
    private int currentRowId;
    private DataPage<T> currentPage;
    private long totalRecords;

    public ScanOperator(BufferManagerImpl bufferManager, int catalogIndex) {
        this.bufferManager = bufferManager;
        this.catalogIndex = catalogIndex;
        this.totalRecords = 0;
    }

    @Override
    public void open() {
        currentPageId = 0;
        currentRowId = 0;
        currentPage = (DataPage<T>)(bufferManager.getPage(currentPageId, catalogIndex));
    }

    @Override
    public T next() {
        while (currentPage != null) {
            T record = currentPage.getRecord(currentRowId);
            currentRowId++;

            if (record != null) {
                totalRecords++;
                return record;
            } else {
                bufferManager.unpinPage(currentPageId, catalogIndex);
                currentPageId++;
                currentRowId = 0;
                currentPage = (DataPage<T>)(bufferManager.getPage(currentPageId, catalogIndex));
            }
        }
        return null; // end of table
    }

    @Override
    public void close() {
        if (currentPage != null) {
            bufferManager.unpinPage(currentPageId, catalogIndex);
        }
    }

    public long getTotalRecords(){
        return totalRecords;
    }
}