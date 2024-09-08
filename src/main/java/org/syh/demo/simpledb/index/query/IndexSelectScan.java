package org.syh.demo.simpledb.index.query;

import org.syh.demo.simpledb.index.Index;
import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.query.TableScan;

public class IndexSelectScan implements Scan {
    private TableScan tableScan;
    private Index index;
    private Constant val;

    public IndexSelectScan(TableScan tableScan, Index index, Constant val) {
        this.tableScan = tableScan;
        this.index = index;
        this.val = val;
        beforeFirst();
    }

    @Override
    public void beforeFirst() {
        index.beforeFirst(val);
    }

    @Override
    public boolean next() {
        boolean ok = index.next();
        if (ok) {
            tableScan.moveToRid(index.getDataRid());
        }
        return ok;
    }

    @Override
    public int getInt(String fieldName) {
        return tableScan.getInt(fieldName);
    }

    @Override
    public String getString(String fieldName) {
        return tableScan.getString(fieldName);
    }

    @Override
    public Constant getVal(String fieldName) {
        return tableScan.getVal(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return tableScan.hasField(fieldName);
    }

    @Override
    public void close() {
        index.close();
        tableScan.close();
    }
}
