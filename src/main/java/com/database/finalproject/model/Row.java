package com.database.finalproject.model;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.database.finalproject.constants.PageConstants.PADDING_BYTE;

public class Row {
    private byte[] movieId;
    private byte[] title;

    public Row(byte[] movieId, byte[] title) {
        this.movieId = truncateOrPadByteArray(movieId,9);
        this.title = truncateOrPadByteArray(title,30);
        //System.out.println(this.movieId.length + " " + this.title.length);
    }

    public byte[] getMovieId() {
        return movieId;
    }

    public byte[] getTitle() {
        return title;
    }

    private static byte[] truncateOrPadByteArray(byte[] value, int maxLength) {

        if (value.length > maxLength) {
            return Arrays.copyOf(value, maxLength); // Truncate safely at byte level
        } else {
            byte[] padded = new byte[maxLength];
            System.arraycopy(value, 0, padded, 0, value.length); // Copy original bytes
            Arrays.fill(padded, value.length, maxLength, PADDING_BYTE); // Fill remaining space with 0
            return padded;
        }



    }
}
