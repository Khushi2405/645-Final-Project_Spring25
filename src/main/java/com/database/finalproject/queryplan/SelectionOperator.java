package com.database.finalproject.queryplan;

import com.database.finalproject.model.Comparator;
import com.database.finalproject.model.SelectionPredicate;
import com.database.finalproject.model.record.ParentRecord;

import java.util.List;

public class SelectionOperator<T extends ParentRecord> implements Operator<T> {

    private final Operator<T> childOperator;
    private final List<SelectionPredicate> predicates;

    private long  matchCount;

    public SelectionOperator(Operator<T> childOperator, List<SelectionPredicate> predicates) {
        this.childOperator = childOperator;
        this.predicates = predicates;
        this.matchCount = 0;
    }

    @Override
    public void open() {
        childOperator.open();
    }

    @Override
    public T next() {
        T record;
        while ((record = (T) childOperator.next()) != null) {
            if (matchesAllPredicates(record)) {
                matchCount++;
                return record;
            }
        }
        return null;
    }

    @Override
    public void close() {
        childOperator.close();
    }

    private boolean matchesAllPredicates(T record) {
        for (SelectionPredicate predicate : predicates) {
            String fieldValue = record.getFieldByIndex(predicate.attributeIndex());
            if (!compare(fieldValue, predicate.valueToCompare(), predicate.comparator())) {
                return false; // fail even one predicate
            }
        }
        return true;
    }

    private boolean compare(String fieldValue, String valueToCompare, Comparator comparator) {
        int cmp = fieldValue.compareTo(valueToCompare);

        return switch (comparator) {
            case EQUALS -> cmp == 0;
            case GREATER_THAN -> cmp > 0;
            case GREATER_THAN_OR_EQUALS -> cmp >= 0;
            case LESS_THAN -> cmp < 0;
            case LESS_THAN_OR_EQUALS -> cmp <= 0;
        };
    }

    public long getTotalMatched(){
        return matchCount;
    }
}
