package org.syh.demo.simpledb.materialize.group;

import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.query.Scan;

public interface AggregationFn {
    void processFirst(Scan scan);
    void processNext(Scan scan);
    String fieldName();
    Constant value();
}
