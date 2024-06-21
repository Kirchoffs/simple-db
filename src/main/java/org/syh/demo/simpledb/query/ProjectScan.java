package org.syh.demo.simpledb.query;

import org.syh.demo.simpledb.parse.Constant;

import java.util.Collection;

public class ProjectScan implements Scan {
    private Scan scan;
    private Collection<String> fieldList;

    public ProjectScan(Scan scan, Collection<String> fieldList) {
        this.scan = scan;
        this.fieldList = fieldList;
    }

    @Override
    public void beforeFirst() {
        scan.beforeFirst();
    }

    @Override
    public boolean next() {
        return scan.next();
    }

    @Override
    public Constant getVal(String fieldName) {
        if (hasField(fieldName)) {
            return scan.getVal(fieldName);
        } else {
            throw new RuntimeException("field " + fieldName + " not found.");
        }
    }

    @Override
    public int getInt(String fieldName) {
        if (hasField(fieldName)) {
            return scan.getInt(fieldName);
        } else {
            throw new RuntimeException("field " + fieldName + " not found.");
        }
    }

    @Override
    public String getString(String fieldName) {
        if (hasField(fieldName)) {
            return scan.getString(fieldName);
        } else {
            throw new RuntimeException("field " + fieldName + " not found.");
        }
    }

    @Override
    public boolean hasField(String fieldName) {
        return fieldList.contains(fieldName);
    }

    @Override
    public void close() {
        scan.close();
    }
}
