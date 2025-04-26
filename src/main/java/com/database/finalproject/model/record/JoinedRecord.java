package com.database.finalproject.model.record;

import java.util.ArrayList;
import java.util.List;

public class JoinedRecord implements ParentRecord {
    private final List<String> fields;

    public JoinedRecord(ParentRecord left, ParentRecord right) {
        fields = new ArrayList<>();
        for (int i = 0;; i++) {
            try {
                fields.add(left.getFieldByIndex(i));
            } catch (Exception e) {
                break;
            }
        }
        for (int i = 0;; i++) {
            try {
                fields.add(right.getFieldByIndex(i));
            } catch (Exception e) {
                break;
            }
        }
    }

    @Override
    public String toString() {
        return String.join(", ", fields);
    }

    @Override
    public String getFieldByIndex(int index) {
        return fields.get(index);
    }
}
