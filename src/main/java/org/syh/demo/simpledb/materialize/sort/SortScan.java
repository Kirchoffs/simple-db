package org.syh.demo.simpledb.materialize.sort;

import org.syh.demo.simpledb.materialize.TempTable;
import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.query.UpdateScan;
import org.syh.demo.simpledb.record.Rid;

import java.util.Arrays;
import java.util.List;
import java.util.Queue;

public class SortScan implements Scan {
    private UpdateScan firstScan, secondScan;
    private UpdateScan currentScan;
    private RecordComparator comparator;
    private boolean hasMoreFirst, hasMoreSecond;
    private List<Rid> savedPositions;

    public SortScan(Queue<TempTable> runs, RecordComparator comparator) {
        this.comparator = comparator;
        firstScan = runs.poll().open();
        hasMoreFirst = firstScan.next();
        if (runs.isEmpty()) {
            secondScan = null;
            hasMoreSecond = false;
        } else {
            secondScan = runs.poll().open();
            hasMoreSecond = secondScan.next();
        }
    }

    @Override
    public void beforeFirst() {
        currentScan = null;
        firstScan.beforeFirst();
        hasMoreFirst = firstScan.next();
        if (secondScan != null) {
            secondScan.beforeFirst();
            hasMoreSecond = secondScan.next();
        }
    }

    @Override
    public boolean next() {
        if (currentScan != null) {
            if (currentScan == firstScan) {
                hasMoreFirst = firstScan.next();
            } else {
                hasMoreSecond = secondScan.next();
            }
        }

        if (!hasMoreFirst && !hasMoreSecond) {
            return false;
        } else if (hasMoreFirst && hasMoreSecond) {
            if (comparator.compare(firstScan, secondScan) < 0) {
                currentScan = firstScan;
            } else {
                currentScan = secondScan;
            }
        } else if (hasMoreFirst) {
            currentScan = firstScan;
        } else {
            currentScan = secondScan;
        }

        return true;
    }

    @Override
    public void close() {
        firstScan.close();
        if (secondScan != null) {
            secondScan.close();
        }
    }

    @Override
    public Constant getVal(String fieldName) {
        return currentScan.getVal(fieldName);
    }

    @Override
    public int getInt(String fieldName) {
        return currentScan.getInt(fieldName);
    }

    @Override
    public String getString(String fieldName) {
        return currentScan.getString(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return currentScan.hasField(fieldName);
    }

    public void savePositions() {
        Rid firstRid = firstScan.getRid();
        Rid secondRid = secondScan == null ? null : secondScan.getRid();
        savedPositions = Arrays.asList(firstRid, secondRid);
    }

    public void restorePositions() {
        firstScan.moveToRid(savedPositions.get(0));
        if (secondScan != null) {
            secondScan.moveToRid(savedPositions.get(1));
        }
    }
}
