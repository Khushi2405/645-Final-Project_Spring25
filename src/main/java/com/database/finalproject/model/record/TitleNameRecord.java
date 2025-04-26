package com.database.finalproject.model.record;

import static com.database.finalproject.constants.PageConstants.removeTrailingBytes;
import static com.database.finalproject.constants.PageConstants.truncateOrPadByteArray;

public record TitleNameRecord(byte[] movieTitle, byte[] personName) implements ParentRecord {
    public TitleNameRecord {
        movieTitle = truncateOrPadByteArray(movieTitle, 30);
        personName = truncateOrPadByteArray(personName, 105);
    }


    @Override
    public String toString() {
        return "MoviePersonRecord{" +
                "title=" + new String(removeTrailingBytes(movieTitle)).trim() +
                ", personName=" + new String(removeTrailingBytes(personName)).trim() +
                '}';
    }

    @Override
    public String getFieldByIndex(int index) {
        return switch (index) {
            case 0 -> new String(removeTrailingBytes(movieTitle)).trim();
            case 1 -> new String(removeTrailingBytes(personName)).trim();
            default -> throw new IllegalArgumentException("Invalid field index for TitleNameRecord: " + index);
        };
    }
}
