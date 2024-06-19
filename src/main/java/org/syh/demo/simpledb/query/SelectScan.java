package org.syh.demo.simpledb.query;

import org.syh.demo.simpledb.record.RID;

public class SelectScan implements UpdateScan {
    private Scan scan;
    private Predicate predicate;

    public SelectScan(Scan scan, Predicate predicate) {
        this.scan = scan;
        this.predicate = predicate;
    }

    @Override
    public void beforeFirst() {
        scan.beforeFirst();
    }

    @Override
    public boolean next() {
        while (scan.next()) {
            if (predicate.isSatisfied(scan)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int getInt(String fieldName) {
        return scan.getInt(fieldName);
    }

    @Override
    public String getString(String fieldName) {
        return scan.getString(fieldName);
    }

    @Override
    public Constant getVal(String fieldName) {
        return scan.getVal(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return scan.hasField(fieldName);
    }

    @Override
    public void close() {
        scan.close();
    }

    @Override
    public void setVal(String fieldName, Constant val) {
        UpdateScan updateScan = (UpdateScan) scan;
        updateScan.setVal(fieldName, val);
    }

    @Override
    public void setInt(String fieldName, int val) {
        UpdateScan updateScan = (UpdateScan) scan;
        updateScan.setInt(fieldName, val);
    }

    @Override
    public void setString(String fieldName, String val) {
        UpdateScan updateScan = (UpdateScan) scan;
        updateScan.setString(fieldName, val);
    }

    @Override
    public void insert() {
        UpdateScan updateScan = (UpdateScan) scan;
        updateScan.insert();
    }

    @Override
    public void delete() {
        UpdateScan updateScan = (UpdateScan) scan;
        updateScan.delete();
    }

    @Override
    public RID getRid() {
        UpdateScan updateScan = (UpdateScan) scan;
        return updateScan.getRid();
    }

    @Override
    public void moveToRid(RID rid) {
        UpdateScan updateScan = (UpdateScan) scan;
        updateScan.moveToRid(rid);
    }
}
