package org.syh.demo.simpledb.query;

import org.syh.demo.simpledb.parse.models.Constant;

public class ProductScan implements Scan {
    private Scan scanLeft;
    private Scan scanRight;

    public ProductScan(Scan scanLeft, Scan scanRight) {
        this.scanLeft = scanLeft;
        this.scanRight = scanRight;
    }

    @Override
    public void beforeFirst() {
        scanLeft.beforeFirst();
        scanLeft.next();
        scanRight.beforeFirst();
    }

    @Override
    public boolean next() {
        if (scanRight.next()) {
            return true;
        } else {
            scanRight.beforeFirst();
            return scanLeft.next() && scanRight.next();
        }
    }

    @Override
    public Constant getVal(String fieldName) {
        if (scanLeft.hasField(fieldName)) {
            return scanLeft.getVal(fieldName);
        } else {
            return scanRight.getVal(fieldName);
        }
    }

    @Override
    public int getInt(String fieldName) {
        if (scanLeft.hasField(fieldName)) {
            return scanLeft.getInt(fieldName);
        } else {
            return scanRight.getInt(fieldName);
        }
    }

    @Override
    public String getString(String fieldName) {
        if (scanLeft.hasField(fieldName)) {
            return scanLeft.getString(fieldName);
        } else {
            return scanRight.getString(fieldName);
        }
    }

    @Override
    public boolean hasField(String fieldName) {
        return scanLeft.hasField(fieldName) || scanRight.hasField(fieldName);
    }

    @Override
    public void close() {
        scanLeft.close();
        scanRight.close();
    }
}
