package com.database.finalproject.queryplan;

import com.database.finalproject.model.ProjectionType;
import com.database.finalproject.model.record.MoviePersonRecord;
import com.database.finalproject.model.record.ParentRecord;
import com.database.finalproject.model.record.TitleNameRecord;


public class ProjectionOperator<I extends ParentRecord, O extends ParentRecord> implements Operator<O> {
    private final Operator<I> child;
    private final ProjectionType projectionType;
    private boolean isOpen = false;

    public ProjectionOperator(Operator<I> child, ProjectionType projectionType) {
        this.child = child;
        this.projectionType = projectionType;
    }

    @Override
    public void open() {
        child.open();
        isOpen = true;
    }

    @Override
    public O next() {
        I record = child.next();
        if (record == null) return null;

        return (O) switch (projectionType) {
            case PROJECTION_ON_WORKED_ON -> {
                byte[] movieId = record.getFieldByIndex(0).getBytes();
                byte[] personId = record.getFieldByIndex(1).getBytes();
                yield new MoviePersonRecord(movieId, personId);
            }
            case PROJECTION_ON_FINAL_JOIN -> {
                byte[] title = record.getFieldByIndex(2).getBytes(); // assuming join record layout
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
