package org.syh.demo.simpledb.materialize.group;

import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.query.Scan;

import java.util.List;

public class GroupByScan implements Scan {
    private Scan scan;
    private List<String> groupFields;
    private List<AggregationFn> aggregationFns;
    private GroupValue groupValue;
    private boolean moreGroups;

    public GroupByScan(Scan scan, List<String> groupFields, List<AggregationFn> aggregationFns) {
        this.scan = scan;
        this.groupFields = groupFields;
        this.aggregationFns = aggregationFns;
        beforeFirst();
    }

    @Override
    public void beforeFirst() {
        scan.beforeFirst();
        moreGroups = scan.next();
    }

    @Override
    public boolean next() {
        if (!moreGroups) {
            return false;
        }

        for (AggregationFn aggregationFn : aggregationFns) {
            aggregationFn.processFirst(scan);
        }

        while (moreGroups = scan.next()) {
            GroupValue nextGroupValue = new GroupValue(scan, groupFields);
            if (!groupValue.equals(nextGroupValue)) {
                break;
            }
            for (AggregationFn aggregationFn : aggregationFns) {
                aggregationFn.processNext(scan);
            }
        }

        return true;
    }

    @Override
    public void close() {
        scan.close();
    }

    @Override
    public Constant getVal(String fieldName) {
        if (groupFields.contains(fieldName)) {
            return groupValue.getVal(fieldName);
        }

        for (AggregationFn aggregationFn : aggregationFns) {
            if (aggregationFn.fieldName().equals(fieldName)) {
                return aggregationFn.value();
            }
        }

        throw new RuntimeException("Field " + fieldName + " not found.");
    }

    @Override
    public int getInt(String fieldName) {
        return getVal(fieldName).asInt();
    }

    @Override
    public String getString(String fieldName) {
        return getVal(fieldName).asString();
    }

    @Override
    public boolean hasField(String fieldName) {
        return groupFields.contains(fieldName) || aggregationFns.stream().anyMatch(aggregationFn -> aggregationFn.fieldName().equals(fieldName));
    }
}
