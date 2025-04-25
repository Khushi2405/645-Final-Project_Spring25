package com.database.finalproject.queryplan;

import com.database.finalproject.model.Comparator;
import com.database.finalproject.model.record.ParentRecord;

public class SelectionOperator<T extends ParentRecord> implements Operator {

    private final Operator childOperator;
    private final int attributeIndex;
    private final String valueToCompare;
    private final Comparator comparator;

    public SelectionOperator(Operator childOperator, int attributeIndex, String valueToCompare, Comparator comparator) {
        this.childOperator = childOperator;
        this.attributeIndex = attributeIndex;
        this.valueToCompare = valueToCompare;
        this.comparator = comparator;
    }

    @Override
    public void open() {
        childOperator.open();
    }

    @Override
    public T next() {
        T record;
        while ((record = (T) childOperator.next()) != null) {
            String fieldValue = record.getFieldByIndex(attributeIndex);
            if (compare(fieldValue, valueToCompare)) {
                return record;
            }
        }
        return null;
    }

    @Override
    public void close() {
        childOperator.close();
    }

    private boolean compare(String fieldValue, String valueToCompare) {
        int cmp = fieldValue.compareTo(valueToCompare);

        return switch (comparator) {
            case EQUALS -> cmp == 0;
            case GREATER_THAN -> cmp > 0;
            case GREATER_THAN_OR_EQUALS -> cmp >= 0;
            case LESS_THAN -> cmp < 0;
            case LESS_THAN_OR_EQUALS -> cmp <= 0;
        };
    }
}
