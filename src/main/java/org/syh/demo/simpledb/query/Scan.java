package org.syh.demo.simpledb.query;

import org.syh.demo.simpledb.parse.models.Constant;

public interface Scan {
    void beforeFirst();
    boolean next();
    Constant getVal(String fieldName);
    int getInt(String fieldName);
    String getString(String fieldName);
    boolean hasField(String fieldName);
    void close();
}
