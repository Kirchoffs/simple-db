package org.syh.demo.simpledb.query;

public interface Scan {
    void beforeFirst();
    boolean next();
    Constant getVal(String fieldName);
    int getInt(String fieldName);
    String getString(String fieldName);
    boolean hasField(String fieldName);
    void close();
}
