package com.database.finalproject.model;

import static com.database.finalproject.constants.PageConstants.PADDING_BYTE;

import java.util.Arrays;

public record Row(byte[] movieId, byte[] title) {
    @Override
    public String toString() {
        return "Row{" +
                "movieId=" + new String(movieId).trim() +
                ", title=" + new String(title).trim() +
                '}';
    }

    public Row(byte[] movieId, byte[] title) {
        this.movieId = movieId;
        this.title = title;
    }


}
