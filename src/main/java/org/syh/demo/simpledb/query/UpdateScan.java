package org.syh.demo.simpledb.query;

import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.record.Rid;

public interface UpdateScan extends Scan {
    void setVal(String fieldName, Constant val);
    void setInt(String fieldName, int val);
    void setString(String fieldName, String val);
    void insert();
    void delete();
    Rid getRid();
    void moveToRid(Rid rid);
}
