package org.syh.demo.simpledb.materialize.merge;

import org.syh.demo.simpledb.materialize.sort.SortScan;
import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.query.Scan;

public class MergeJoinScan implements Scan {
    private Scan leftScan;
    private SortScan rightScan;
    private String leftField, rightField;
    private Constant joinVal;

    public MergeJoinScan(Scan leftScan, SortScan rightScan, String leftField, String rightField) {
        this.leftScan = leftScan;
        this.rightScan = rightScan;
        this.leftField = leftField;
        this.rightField = rightField;
        beforeFirst();
    }

    @Override
    public void close() {
        leftScan.close();
        rightScan.close();
    }

    @Override
    public void beforeFirst() {
        leftScan.beforeFirst();
        rightScan.beforeFirst();
    }

    @Override
    public boolean next() {
        boolean hasMoreForRightScan = rightScan.next();
        if (hasMoreForRightScan && rightScan.getVal(rightField).equals(joinVal)) {
            return true;
        }

        boolean hasMoreForLeftScan = leftScan.next();
        if (hasMoreForLeftScan && leftScan.getVal(leftField).equals(joinVal)) {
            rightScan.restorePositions();
            return true;
        }

        while (hasMoreForLeftScan && hasMoreForRightScan) {
            Constant leftVal = leftScan.getVal(leftField);
            Constant rightVal = rightScan.getVal(rightField);
            if (leftVal.compareTo(rightVal) < 0) {
                hasMoreForLeftScan = leftScan.next();
            } else if (leftVal.compareTo(rightVal) > 0) {
                hasMoreForRightScan = rightScan.next();
            } else {
                rightScan.savePositions();
                joinVal = rightScan.getVal(rightField);
                return true;
            }
        }

        return false;
    }

    @Override
    public int getInt(String field) {
        if (leftScan.hasField(field)) {
            return leftScan.getInt(field);
        } else {
            return rightScan.getInt(field);
        }
    }

    @Override
    public String getString(String field) {
        if (leftScan.hasField(field)) {
            return leftScan.getString(field);
        } else {
            return rightScan.getString(field);
        }
    }

    @Override
    public Constant getVal(String field) {
        if (leftScan.hasField(field)) {
            return leftScan.getVal(field);
        } else {
            return rightScan.getVal(field);
        }
    }

    @Override
    public boolean hasField(String field) {
        return leftScan.hasField(field) || rightScan.hasField(field);
    }
}
