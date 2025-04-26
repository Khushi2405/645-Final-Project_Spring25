package com.database.finalproject.queryplan;

import com.database.finalproject.buffermanager.BufferManagerImpl;
import com.database.finalproject.model.page.DataPage;
import com.database.finalproject.model.record.ParentRecord;

public class MaterializeOperator<T extends ParentRecord> implements Operator {
    private final Operator child;
    private final BufferManagerImpl bufferManager;
    private final int catalogIndex;
    private DataPage<T> tempPage;
    private DataPage<T> currentPage;
    private int currentPageId;
    private int currentRowId;
    private boolean isMaterialized;
    private boolean isOpen = false;

    public MaterializeOperator(Operator child, BufferManagerImpl bufferManager, int catalogIndex) {
        this.child = child;
        this.bufferManager = bufferManager;
        this.catalogIndex = catalogIndex;
//        this.tempPages = new ArrayList<>();
        isMaterialized = bufferManager.getTotalPages(catalogIndex) == 0;
        this.currentPageId = 0;
        this.currentRowId = 0;
    }

    @Override
    public void open() {
        child.open();
        isOpen = true;
    }

    @Override
    public T next() {
        if (!isMaterialized) {
            // Try reading from child and materializing
            T record = (T) child.next();
            if (record == null) {
                bufferManager.unpinPage(tempPage.getPid(), catalogIndex);
                isMaterialized = true; // end of child
                resetScan();
                return next();
            }

            writeToTemp(record);
            return record; // pipeline and materialize first
        } else {
            // After materialization, scan from temp
            while (currentPage != null) {
                T record = currentPage.getRecord(currentRowId);
                currentRowId++;

                if (record != null) {
                    return record;
                } else {
                    bufferManager.unpinPage(currentPageId, catalogIndex);
                    currentPageId++;
                    currentRowId = 0;
                    currentPage = (DataPage<T>)(bufferManager.getPage(currentPageId, catalogIndex));
                }
            }
            resetScan();
            return null;
        }
    }

    @Override
    public void close() {
        if (isOpen) {
            child.close();
            isOpen = false;
        }
    }

    private void writeToTemp(T record) {
        if(tempPage == null)
            tempPage = (DataPage<T>) bufferManager.createPage(catalogIndex);
        if (tempPage.isFull()) {
            bufferManager.unpinPage(tempPage.getPid(), catalogIndex);
            tempPage = (DataPage<T>) bufferManager.createPage(catalogIndex);

        }
        tempPage.insertRecord(record);
    }

    private void resetScan() {
        currentPageId = 0;
        currentRowId = 0;
        currentPage = (DataPage<T>)(bufferManager.getPage(currentPageId, catalogIndex));
    }
}