package com.database.finalproject.queryplan;

import com.database.finalproject.model.record.ParentRecord;
// import com.database.finalproject.model.record.Rid;
import com.database.finalproject.model.record.JoinedRecord;

import java.util.*;

public class BNLJoinOperator<T extends ParentRecord> implements Operator {
    private final Operator leftChild;
    private final Operator rightChild;
    private final int joinAttrLeft;
    private final int joinAttrRight;
    private final int blockSize;
    private final int recordsPerPage;

    private Map<String, List<T>> hashTable;
    private Iterator<T> outerMatchesIterator;
    private T currentRightRecord;

    public BNLJoinOperator(Operator leftChild, Operator rightChild, int joinAttrLeft, int joinAttrRight, int blockSize,
            int recordsPerPage) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.joinAttrLeft = joinAttrLeft;
        this.joinAttrRight = joinAttrRight;
        this.blockSize = blockSize;
        this.recordsPerPage = recordsPerPage;
    }

    @Override
    public void open() {
        leftChild.open();
        rightChild.open();
        hashTable = new HashMap<>();
        loadNextLeftBlock();
    }

    @Override
    public T next() {
        while (true) {
            if (outerMatchesIterator != null && outerMatchesIterator.hasNext()) {
                T outerRecord = outerMatchesIterator.next();
                return joinRecords(outerRecord, currentRightRecord);
            }

            currentRightRecord = (T) rightChild.next();
            if (currentRightRecord == null) {
                if (!loadNextLeftBlock()) {
                    return null; // All data processed
                }
                currentRightRecord = (T) rightChild.next();
                if (currentRightRecord == null) {
                    return null; // No more right-side data
                }
            }

            String key = currentRightRecord.getFieldByIndex(joinAttrRight);
            List<T> matchingOuter = hashTable.getOrDefault(key, new ArrayList<>());
            outerMatchesIterator = matchingOuter.iterator();
        }
    }

    @Override
    public void close() {
        leftChild.close();
        rightChild.close();
        hashTable.clear();
    }

    private boolean loadNextLeftBlock() {
        hashTable.clear();
        rightChild.close();
        rightChild.open();

        int count = 0;
        int maxRecordsInBlock = blockSize * recordsPerPage;
        while (count < maxRecordsInBlock) {
            T record = (T) leftChild.next();
            if (record == null)
                break;
            String key = record.getFieldByIndex(joinAttrLeft);
            hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(record);
            count++;
        }
        return !hashTable.isEmpty();
    }

    private T joinRecords(T left, T right) {
        // Implement how to combine two records from left and right child into a new
        // record.
        // You may use a custom `JoinedRecord` class that implements `ParentRecord`.
        return (T) new JoinedRecord(left, right);
    }
}
