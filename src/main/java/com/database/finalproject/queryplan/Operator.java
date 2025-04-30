package com.database.finalproject.queryplan;

import com.database.finalproject.model.record.ParentRecord;

public interface Operator<T extends ParentRecord> {
    void open();
    T next();
    void close();
}
