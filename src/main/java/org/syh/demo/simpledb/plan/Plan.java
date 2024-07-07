package org.syh.demo.simpledb.plan;

import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.record.Schema;

public interface Plan {
    Scan open();
    int blocksAccessed();
    int recordsOutput();
    int distinctValues(String fieldName);
    Schema getSchema();
}
