package com.database.finalproject.model.record;

import static com.database.finalproject.constants.PageConstants.*;

public record MovieWorkedOnPeopleJoinRecord(byte[] movieId, byte[] personId, byte[] title, byte[] name) implements ParentRecord {

    public MovieWorkedOnPeopleJoinRecord {
        movieId = truncateOrPadByteArray(movieId, MOVIE_ID_SIZE);
        personId = truncateOrPadByteArray(personId, PERSON_ID_SIZE);
        title = truncateOrPadByteArray(title, MOVIE_TITLE_SIZE);
        name = truncateOrPadByteArray(name, PERSON_NAME_SIZE);
    }


    @Override
    public String toString() {
        return "MovieWorkedOnPeopleJoinRecord{" +
                "movieId=" + new String(removeTrailingBytes(movieId)).trim() +
                ", personId=" + new String(removeTrailingBytes(personId)).trim() +
                ", title=" + new String(removeTrailingBytes(title)).trim() +
                ", name=" + new String(removeTrailingBytes(name)).trim() +
                '}';
    }

    @Override
    public String getFieldByIndex(int index) {
        return switch (index) {
            case 0 -> new String(removeTrailingBytes(movieId)).trim();
            case 1 -> new String(removeTrailingBytes(personId)).trim();
            case 2 -> new String(removeTrailingBytes(title)).trim();
            case 3 -> new String(removeTrailingBytes(name)).trim();
            default -> throw new IllegalArgumentException("Invalid field index for WorkedOnRecord: " + index);
        };
    }
}