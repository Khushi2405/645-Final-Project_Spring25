package com.database.finalproject.model.record;

import static com.database.finalproject.constants.PageConstants.*;

public record MovieWorkedOnJoinRecord(byte[] movieId, byte[] personId, byte[] title) implements ParentRecord {

    public MovieWorkedOnJoinRecord{
        movieId = truncateOrPadByteArray(movieId, MOVIE_ID_SIZE);
        personId = truncateOrPadByteArray(personId, PERSON_ID_SIZE);
        title = truncateOrPadByteArray(title, MOVIE_TITLE_SIZE);
    }


    @Override
    public String toString() {
        return "WorkedOnRecord{" +
                "movieId=" + new String(removeTrailingBytes(movieId)).trim() +
                ", personId=" + new String(removeTrailingBytes(personId)).trim() +
                ", title=" + new String(removeTrailingBytes(title)).trim() +
                '}';
    }

    @Override
    public String getFieldByIndex(int index) {
        return switch (index) {
            case 0 -> new String(removeTrailingBytes(movieId)).trim();
            case 1 -> new String(removeTrailingBytes(personId)).trim();
            case 2 -> new String(removeTrailingBytes(title)).trim();
            default -> throw new IllegalArgumentException("Invalid field index for WorkedOnRecord: " + index);
        };
    }
}