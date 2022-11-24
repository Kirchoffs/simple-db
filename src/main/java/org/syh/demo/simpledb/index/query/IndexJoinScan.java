package org.syh.demo.simpledb.index.query;

import org.syh.demo.simpledb.index.Index;
import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.query.TableScan;

public class IndexJoinScan implements Scan {
    private Scan leftScan;
    private Index rightIndex;
    private String joinField;
    private TableScan rightTableScan;

    public IndexJoinScan(Scan leftScan, Index rightIndex, String joinField, TableScan rightTableScan) {
        this.leftScan = leftScan;
        this.rightIndex = rightIndex;
        this.joinField = joinField;
        this.rightTableScan = rightTableScan;
        beforeFirst();
    }

    public void beforeFirst() {
        leftScan.beforeFirst();
        leftScan.next();
        resetIndex();           // resetIndex() will call beforeFirst() so leftScan does not need to call beforeFirst()
    }

    public boolean next() {
        while (true) {
            if (rightIndex.next()) {
                rightTableScan.moveToRid(rightIndex.getDataRid());
                return true;
            }
            if (!leftScan.next()) {
                return false;
            }
            resetIndex();
        }
    }

    public int getInt(String fieldName) {
        if (rightTableScan.hasField(fieldName)) {
            return rightTableScan.getInt(fieldName);
        } else {
            return leftScan.getInt(fieldName);
        }
    }

    public String getString(String fieldName) {
        if (rightTableScan.hasField(fieldName)) {
            return rightTableScan.getString(fieldName);
        } else {
            return leftScan.getString(fieldName);
        }
    }

    public Constant getVal(String fieldName) {
        if (rightTableScan.hasField(fieldName)) {
            return rightTableScan.getVal(fieldName);
        } else {
            return leftScan.getVal(fieldName);
        }
    }

    public boolean hasField(String fieldName) {
        return rightTableScan.hasField(fieldName) || leftScan.hasField(fieldName);
    }

    private void resetIndex() {
        Constant searchKey = leftScan.getVal(joinField);
        rightIndex.beforeFirst(searchKey);
    }

    public void close() {
        leftScan.close();
        rightIndex.close();
        rightTableScan.close();
    }
}
