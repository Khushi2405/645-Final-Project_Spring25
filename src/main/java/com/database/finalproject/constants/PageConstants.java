package com.database.finalproject.constants;

public class PageConstants {
    public static final int PAGE_SIZE = 4 * 1024; // 4 KB
    public static final int ROW_SIZE = 39; // 9 bytes for movieId + 30 bytes for title
    public static final int PAGE_ROW_LIMIT = PAGE_SIZE / ROW_SIZE;
    public static final byte PADDING_BYTE = 0x7F;
    public static final int MOVIE_ID_SIZE = 9; // Movie ID size (9 bytes)
    public static final int MOVIE_TITLE_SIZE = 30;
    public static final int PAGE_ID_SIZE = 4; // Page ID size (4 bytes)
    public static final int SLOT_ID_SIZE = 1;

    public static final int MOVIE_ID_NON_LEAF_NODE_ORDER = 314;
    public static final int MOVIE_ID_LEAF_NODE_ORDER = 291;
    public static final int MOVIE_TITLE_NON_LEAF_NODE_ORDER = 120;
    public static final int MOVIE_TITLE_LEAF_NODE_ORDER = 116;
    public static final int DATA_PAGE_INDEX = 0;
    public static final int MOVIE_ID_INDEX_PAGE_INDEX = 1;
    public static final int MOVIE_TITLE_INDEX_INDEX = 2;

    public static final String DATA_INPUT_FILE = "src/main/resources/static/data_binary_heap.bin";
    public static final String DATABASE_FILE = "src/main/resources/static/title.basics.tsv";

    public static final String DATABASE_CATALOGUE_KEY_FILENAME = "filename";
    public static final String DATABASE_CATALOGUE_KEY_TOTAL_PAGES = "totalPages";
    public static final String DATABASE_CATALOGUE_KEY_ROOT_PAGE = "rootPage";

    public static final int ATTR_TYPE_ID = 0;
    public static final int ATTR_TYPE_TITLE = 1;

}
