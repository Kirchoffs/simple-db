package org.syh.demo.simpledb.index.hash;

import org.syh.demo.simpledb.index.Index;
import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.query.TableScan;
import org.syh.demo.simpledb.record.Layout;
import org.syh.demo.simpledb.record.Rid;
import org.syh.demo.simpledb.transaction.Transaction;

public class HashIndex implements Index {
    public static int DEFAULT_NUM_BUCKETS = 100;

    private Transaction tx;
    private String idxName;
    private Layout layout;
    private int numBuckets;
    private Constant searchKey;
    private TableScan ts;

    public HashIndex(Transaction tx, String idxName, Layout layout, int numBuckets) {
        this.tx = tx;
        this.idxName = idxName;
        this.layout = layout;
        this.numBuckets = numBuckets;
    }

    public HashIndex(Transaction tx, String idxName, Layout layout) {
        this(tx, idxName, layout, DEFAULT_NUM_BUCKETS);
    }

    @Override
    public void beforeFirst(Constant searchKey) {
        close();
        this.searchKey = searchKey;
        int bucket = searchKey.hashCode() % DEFAULT_NUM_BUCKETS;
        String tableName = String.format("%s-%d", idxName, bucket);
        ts = new TableScan(tx, tableName, layout);
    }

    @Override
    public boolean next() {
        while (ts.next()) {
            if (ts.getVal("dataVal").equals(searchKey)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Rid getDataRid() {
        int blockNum = ts.getInt("blockNum");
        int slot = ts.getInt("slot");
        return new Rid(blockNum, slot);
    }

    @Override
    public void insert(Constant dataVal, Rid dataRid) {
        beforeFirst(dataVal);
        ts.insert();
        ts.setInt("blockNum", dataRid.blockNum());
        ts.setInt("slot", dataRid.slot());
        ts.setVal("dataVal", dataVal);
    }

    @Override
    public void delete(Constant dataVal, Rid dataRid) {
        beforeFirst(dataVal);
        while (next()) {
            if (getDataRid().equals(dataRid)) {
                ts.delete();
                return;
            }
        }
    }

    @Override
    public void close() {
        if (ts != null) {
            ts.close();
        }
    }

    @Override
    public int searchCost(int numBlocks, int numBuckets) {
        return numBlocks / numBuckets;
    }
}
