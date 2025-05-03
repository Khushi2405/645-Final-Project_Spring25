package com.database.finalproject.model.record;


import com.database.finalproject.model.record.ParentRecord;

import static com.database.finalproject.constants.PageConstants.*;

public record MovieRecord(byte[] movieId, byte[] movieTitle) implements ParentRecord {
    @Override
    public String toString() {
        return "MovieRecord{" +
                "movieId=" + new String(removeTrailingBytes(movieId)).trim() +
                ", title=" + new String(removeTrailingBytes(movieTitle)).trim() +
                '}';
    }

    public MovieRecord {
        movieId = truncateOrPadByteArray(movieId, MOVIE_ID_SIZE);
        movieTitle = truncateOrPadByteArray(movieTitle, MOVIE_TITLE_SIZE);
    }

    @Override
    public String getFieldByIndex(int index) {
        return switch (index) {
            case 0 -> new String(removeTrailingBytes(movieId)).trim();
            case 1 -> new String(removeTrailingBytes(movieTitle)).trim();
            default -> throw new IllegalArgumentException("Invalid field index for MovieRecord: " + index);
        };
    }

//
}
