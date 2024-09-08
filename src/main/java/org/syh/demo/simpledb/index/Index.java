package org.syh.demo.simpledb.index;

import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.record.Rid;

public interface Index {
    void beforeFirst(Constant searchKey);
    boolean next();
    Rid getDataRid();
    void insert(Constant dataVal, Rid dataRid);
    void delete(Constant dataVal, Rid dataRid);
    void close();
    int searchCost(int numBlocks, int rpb);
}
