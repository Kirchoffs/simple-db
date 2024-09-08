package org.syh.demo.simpledb.index.btree;

import org.syh.demo.simpledb.parse.models.Constant;

public class DirectoryEntry {
    private Constant dataVal;
    private int blockNum;

    public DirectoryEntry(Constant dataVal, int blockNum) {
        this.dataVal  = dataVal;
        this.blockNum = blockNum;
    }

    public Constant dataVal() {
        return dataVal;
    }

    public int blockNumber() {
        return blockNum;
    }
}
