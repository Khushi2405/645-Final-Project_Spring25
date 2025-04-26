package com.database.finalproject.model.record;

import static com.database.finalproject.constants.PageConstants.removeTrailingBytes;
import static com.database.finalproject.constants.PageConstants.truncateOrPadByteArray;

public record MovieWorkedOnJoinRecord(byte[] movieId, byte[] personId, byte[] title) implements ParentRecord {

    public MovieWorkedOnJoinRecord{
        movieId = truncateOrPadByteArray(movieId, 9);
        personId = truncateOrPadByteArray(personId, 10);
        title = truncateOrPadByteArray(title, 30);
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