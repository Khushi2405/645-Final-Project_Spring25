package com.database.finalproject.constants;

import java.nio.ByteBuffer;
import java.util.Arrays;

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

    public static byte[] truncateOrPadByteArray(byte[] value, int maxLength) {
        if (value.length > maxLength) {
            return Arrays.copyOf(value, maxLength); // Truncate safely at byte level
        } else {
            byte[] padded = new byte[maxLength];
            System.arraycopy(value, 0, padded, 0, value.length); // Copy original bytes
            Arrays.fill(padded, value.length, maxLength, PADDING_BYTE); // Fill remaining space with 0x7F
            return padded;
        }
    }

    public static byte[] removeTrailingBytes(byte[] input) {
        int endIndex = input.length;
        for (int i = input.length - 1; i >= 0; i--) {
            if (input[i] != PADDING_BYTE) {  // Only remove custom padding byte
                endIndex = i + 1;
                break;
            }
        }
        return Arrays.copyOf(input,endIndex);
    }

    public static byte[] intToBytes(int value, int capacity) {
        ByteBuffer buffer = ByteBuffer.allocate(4); // Always allocate 4 bytes
        buffer.putInt(value);
        return Arrays.copyOfRange(buffer.array(), 4 - capacity, 4); // Extract the required bytes
    }

    // Convert a 4-byte array back to an integer
    public static int bytesToInt(byte[] bytes) {
        // return ByteBuffer.wrap(bytes).getInt();
        ByteBuffer buffer = ByteBuffer.allocate(4); // Ensure 4 bytes
        buffer.put(new byte[4 - bytes.length]); // Pad with leading zeros if needed
        buffer.put(bytes); // Copy the actual bytes
        buffer.rewind(); // Reset position before reading
        return buffer.getInt();
    }
}
