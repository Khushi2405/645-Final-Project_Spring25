package com.database.finalproject.model;


import static com.database.finalproject.constants.PageConstants.*;

public record Row(byte[] movieId, byte[] movieTitle) {
    @Override
    public String toString() {
        return "Row{" +
                "movieId=" + new String(removeTrailingBytes(movieId)).trim() +
                ", title=" + new String(removeTrailingBytes(movieTitle)).trim() +
                '}';
    }

    public Row {
        movieId = truncateOrPadByteArray(movieId, 9);
        movieTitle = truncateOrPadByteArray(movieTitle, 30);
    }



//
}
