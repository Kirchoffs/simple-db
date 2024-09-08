package org.syh.demo.simpledb.index.hash;

import org.syh.demo.simpledb.index.Index;
import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.record.Rid;

public class ExtendibleHashIndex implements Index {
    @Override
    public void beforeFirst(Constant searchKey) {

    }

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public Rid getDataRid() {
        return null;
    }

    @Override
    public void insert(Constant dataVal, Rid dataRid) {

    }

    @Override
    public void delete(Constant dataVal, Rid dataRid) {

    }

    @Override
    public void close() {

    }

    @Override
    public int searchCost(int numBlocks, int rpb) {
        return 0;
    }
}
