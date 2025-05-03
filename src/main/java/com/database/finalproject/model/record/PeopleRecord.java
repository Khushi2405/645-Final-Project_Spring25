package com.database.finalproject.model.record;

import com.database.finalproject.model.record.ParentRecord;

import static com.database.finalproject.constants.PageConstants.*;

public record PeopleRecord(byte[] personId, byte[] name) implements ParentRecord {
    public PeopleRecord {
        personId = truncateOrPadByteArray(personId, PERSON_ID_SIZE);
        name = truncateOrPadByteArray(name, PERSON_NAME_SIZE);
    }

    @Override
    public String toString() {
        return "MovieRecord{" +
                "personId=" + new String(removeTrailingBytes(personId)).trim() +
                ", name=" + new String(removeTrailingBytes(name)).trim() +
                '}';
    }

    @Override
    public String getFieldByIndex(int index) {
        return switch (index) {
            case 0 -> new String(removeTrailingBytes(personId)).trim();
            case 1 -> new String(removeTrailingBytes(name)).trim();
            default -> throw new IllegalArgumentException("Invalid field index for PeopleRecord: " + index);
        };
    }

}