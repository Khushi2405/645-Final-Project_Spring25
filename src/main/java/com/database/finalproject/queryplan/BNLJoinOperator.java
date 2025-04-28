package com.database.finalproject.queryplan;

import com.database.finalproject.model.record.ParentRecord;
import com.database.finalproject.model.record.PeopleRecord;
import com.database.finalproject.model.record.WorkedOnRecord;
import com.database.finalproject.buffermanager.BufferManagerImpl;
import com.database.finalproject.model.page.DataPage;
import com.database.finalproject.model.record.MovieRecord;
import com.database.finalproject.model.record.MovieWorkedOnJoinRecord;
import com.database.finalproject.model.record.MovieWorkedOnPeopleJoinRecord;

import static com.database.finalproject.constants.PageConstants.BNL_MOVIE_WORKED_ON_INDEX;
import static com.database.finalproject.constants.PageConstants.BNL_MOVIE_WORKED_ON_PEOPLE_INDEX;

import java.util.*;

public class BNLJoinOperator<T extends ParentRecord> implements Operator {
    private final Operator leftChild;
    private final Operator rightChild;
    private final int joinAttrLeft;
    private final int joinAttrRight;
    private final int blockSize;
    private final int joinResultType;
    private final BufferManagerImpl bufferManager;

    private Map<String, List<T>> hashTable;
    private Iterator<T> outerMatchesIterator;
    private T currentRightRecord;

    public BNLJoinOperator(Operator leftChild, Operator rightChild, int joinAttrLeft, int joinAttrRight, int blockSize,
            BufferManagerImpl bf, int joinResultType) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.joinAttrLeft = joinAttrLeft;
        this.joinAttrRight = joinAttrRight;
        this.blockSize = blockSize;
        this.joinResultType = joinResultType;
        this.bufferManager = bf;
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
                return (T) joinRecords(outerRecord, currentRightRecord);
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
        // TODO: close or reset scan???
        // rightChild.close();
        // rightChild.open();

        unpinLeftBlock();
        DataPage<T> page = (DataPage<T>) bufferManager.createPage(joinResultType);
        while (true) {
            while (!page.isFull()) {
                T record = (T) leftChild.next();
                page.insertRecord(record);
                if (record == null)
                    break;
                String key = record.getFieldByIndex(joinAttrLeft);
                hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(record);
            }
            if (page.getPid() + 1 == blockSize) {
                break;
            }
            page = (DataPage<T>) bufferManager.createPage(joinResultType);
        }
        return !hashTable.isEmpty();
    }

    private void unpinLeftBlock() {
        for (int i = 0; i < blockSize; i++) {
            bufferManager.unpinPage(i, joinResultType);
        }
        bufferManager.resetBlockPageCount(joinResultType);
    }

    private T joinRecords(T left, T right) {
        if (joinResultType == BNL_MOVIE_WORKED_ON_INDEX) {
            MovieRecord movie = (MovieRecord) left;
            WorkedOnRecord workedOn = (WorkedOnRecord) right;

            byte[] movieId = movie.movieId();
            byte[] title = movie.movieTitle();
            byte[] personId = workedOn.personId();
            return (T) new MovieWorkedOnJoinRecord(movieId, personId, title);
        } else if (joinResultType == BNL_MOVIE_WORKED_ON_PEOPLE_INDEX) {
            MovieWorkedOnJoinRecord joined = (MovieWorkedOnJoinRecord) left;
            PeopleRecord person = (PeopleRecord) right;

            byte[] title = joined.title();
            byte[] name = person.name();
            return (T) new MovieWorkedOnPeopleJoinRecord(title, name);
        }

        System.err.println("Incorrect JOIN result type");
        return null;
    }
}
