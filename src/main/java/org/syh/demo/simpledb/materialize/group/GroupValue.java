package org.syh.demo.simpledb.materialize.group;

import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.query.Scan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupValue {
    private Map<String, Constant> values;

    public GroupValue(Scan scan, List<String> groupFields) {
        values = new HashMap<>();
        for (String fieldName : groupFields) {
            values.put(fieldName, scan.getVal(fieldName));
        }
    }

    public Constant getVal(String fieldName) {
        return values.get(fieldName);
    }

    public boolean equals(Object other) {
        GroupValue otherGroupValue = (GroupValue) other;
        for (String fieldName : values.keySet()) {
            if (!values.get(fieldName).equals(otherGroupValue.values.get(fieldName))) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        int hashVal = 0;
        for (Constant value : values.values()) {
            hashVal += value.hashCode();
        }
        return hashVal;
    }
}
