package com.database.finalproject.queryplan;

import com.database.finalproject.model.record.ParentRecord;

public interface Operator {
    void open();
    ParentRecord next();
    void close();
}
