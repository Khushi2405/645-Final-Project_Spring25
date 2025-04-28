package com.database.finalproject.model;

public class SelectionPredicate {
    private final int attributeIndex;
    private final String valueToCompare;
    private final Comparator comparator;

    public SelectionPredicate(int attributeIndex, String valueToCompare, Comparator comparator) {
        this.attributeIndex = attributeIndex;
        this.valueToCompare = valueToCompare;
        this.comparator = comparator;
    }

    public int attributeIndex() {
        return attributeIndex;
    }

    public String valueToCompare() {
        return valueToCompare;
    }

    public Comparator comparator() {
        return comparator;
    }
}