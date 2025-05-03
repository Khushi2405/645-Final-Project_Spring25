package com.database.finalproject.model.record;

import com.database.finalproject.model.record.ParentRecord;

import static com.database.finalproject.constants.PageConstants.*;

public record WorkedOnRecord(byte[] movieId, byte[] personId, byte[] category) implements ParentRecord {
    public WorkedOnRecord {
        movieId = truncateOrPadByteArray(movieId, MOVIE_ID_SIZE);
        personId = truncateOrPadByteArray(personId, PERSON_ID_SIZE);
        category = truncateOrPadByteArray(category, CATEGORY_SIZE);
    }


    @Override
    public String toString() {
        return "WorkedOnRecord{" +
                "movieId=" + new String(removeTrailingBytes(movieId)).trim() +
                ", personId=" + new String(removeTrailingBytes(personId)).trim() +
                ", category=" + new String(removeTrailingBytes(category)).trim() +
                '}';
    }

    @Override
    public String getFieldByIndex(int index) {
        return switch (index) {
            case 0 -> new String(removeTrailingBytes(movieId)).trim();
            case 1 -> new String(removeTrailingBytes(personId)).trim();
            case 2 -> new String(removeTrailingBytes(category)).trim();
            default -> throw new IllegalArgumentException("Invalid field index for WorkedOnRecord: " + index);
        };
    }
}