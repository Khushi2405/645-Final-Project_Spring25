package com.database.finalproject.model.record;

import static com.database.finalproject.constants.PageConstants.*;

public record MoviePersonRecord(byte[] movieId, byte[] personId) implements ParentRecord {
    public MoviePersonRecord {
        movieId = truncateOrPadByteArray(movieId, MOVIE_ID_SIZE);
        personId = truncateOrPadByteArray(personId, PERSON_ID_SIZE);
    }


    @Override
    public String toString() {
        return "MoviePersonRecord{" +
                "movieId=" + new String(removeTrailingBytes(movieId)).trim() +
                ", personId=" + new String(removeTrailingBytes(personId)).trim() +
                '}';
    }

    @Override
    public String getFieldByIndex(int index) {
        return switch (index) {
            case 0 -> new String(removeTrailingBytes(movieId)).trim();
            case 1 -> new String(removeTrailingBytes(personId)).trim();
            default -> throw new IllegalArgumentException("Invalid field index for MoviePersonRecord: " + index);
        };
    }
}
