package com.database.finalproject.model.record;

import static com.database.finalproject.constants.PageConstants.removeTrailingBytes;
import static com.database.finalproject.constants.PageConstants.truncateOrPadByteArray;

public record MoviePersonRecord(byte[] movieId, byte[] personId) implements ParentRecord {
    public MoviePersonRecord {
        movieId = truncateOrPadByteArray(movieId, 9);
        personId = truncateOrPadByteArray(personId, 10);
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
