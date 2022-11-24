package org.syh.demo.simpledb.materialize.sort;

import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.query.Scan;

import java.util.List;

public class RecordComparator {
    private List<String> fieldNames;

    public RecordComparator(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    public int compare(Scan s1, Scan s2) {
        for (String fieldName : fieldNames) {
            Constant val1 = s1.getVal(fieldName);
            Constant val2 = s2.getVal(fieldName);
            int result = val1.compareTo(val2);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }
}
