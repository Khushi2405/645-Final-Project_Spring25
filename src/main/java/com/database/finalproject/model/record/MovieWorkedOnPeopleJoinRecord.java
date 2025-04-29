package com.database.finalproject.model.record;

import static com.database.finalproject.constants.PageConstants.removeTrailingBytes;
import static com.database.finalproject.constants.PageConstants.truncateOrPadByteArray;

public record MovieWorkedOnPeopleJoinRecord(byte[] movieId, byte[] personId, byte[] title, byte[] name) implements ParentRecord {

    public MovieWorkedOnPeopleJoinRecord {
        movieId = truncateOrPadByteArray(movieId, 9);
        personId = truncateOrPadByteArray(personId, 10);
        title = truncateOrPadByteArray(title, 30);
        name = truncateOrPadByteArray(name, 105);
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