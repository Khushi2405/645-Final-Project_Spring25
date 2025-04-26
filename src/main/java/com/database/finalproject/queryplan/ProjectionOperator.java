package com.database.finalproject.queryplan;

import com.database.finalproject.model.ProjectionType;
import com.database.finalproject.model.record.MoviePersonRecord;
import com.database.finalproject.model.record.ParentRecord;
import com.database.finalproject.model.record.TitleNameRecord;


public class ProjectionOperator<T extends ParentRecord> implements Operator {
    private final Operator child;
    private final ProjectionType projectionType;
    private boolean isOpen = false;

    public ProjectionOperator(Operator child, ProjectionType projectionType) {
        this.child = child;
        this.projectionType = projectionType;
    }

    @Override
    public void open() {
        child.open();
        isOpen = true;
    }

    @Override
    public T next() {
        ParentRecord record = (ParentRecord) child.next();
        if (record == null) return null;

        return (T) switch (projectionType) {
            case PROJECTION_ON_WORKED_ON -> {
                byte[] movieId = record.getFieldByIndex(0).getBytes();
                byte[] personId = record.getFieldByIndex(1).getBytes();
                yield new MoviePersonRecord(movieId, personId);
            }
            case PROJECTION_ON_FINAL_JOIN -> {
                byte[] title = record.getFieldByIndex(1).getBytes(); // assuming join record layout
                byte[] name  = record.getFieldByIndex(3).getBytes();
                yield new TitleNameRecord(title, name);
            }
        };
    }

    @Override
    public void close() {
        if (isOpen) {
            child.close();
            isOpen = false;
        }
    }
}
