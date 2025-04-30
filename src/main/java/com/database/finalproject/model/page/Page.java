package com.database.finalproject.model.page;

import static com.database.finalproject.constants.PageConstants.PAGE_SIZE;

public interface Page {
//    MovieRecord getRow(int rowId);
//    int insertRow(MovieRecord row);
//    boolean isFull();

    /**
     * Returns the page id
     * @return page id of this page
     */

    int getPid();
    byte[] getByteArray();


}
