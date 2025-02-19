package com.database.finalproject.model;

public class Row {
    private byte[] movieId;
    private byte[] title;

    public Row(byte[] movieId, byte[] title) {
        this.movieId = movieId;
        this.title = title;
    }

    public byte[] getMovieId() {
        return movieId;
    }

    public byte[] getTitle() {
        return title;
    }
}
