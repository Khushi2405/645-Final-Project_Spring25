package com.database.finalproject.constants;

public class PageConstants {
    public static final int PAGE_SIZE = 4 * 1024; // 4 KB
    public static final int ROW_SIZE = 39; // 9 bytes for movieId + 30 bytes for title
    public static final int PAGE_ROW_LIMIT = PAGE_SIZE / ROW_SIZE;
    public static final int REMAINING_BYTES = PAGE_SIZE % ROW_SIZE; // 1 byte
    public static final byte PADDING_BYTE = 0x7F;
    public static final byte[] EXTRA_BYTE = new byte[] { PADDING_BYTE };

    public static final String INPUT_FILE = "src/main/resources/static/binary_heap.bin";
    public static final String ID_INDEX_FILE = "src/main/resources/static/idBTree.bin";
    public static final String TITLE_INDEX_FILE = "src/main/resources/static/titleBTree.bin";

}
