package org.syh.demo.simpledb.plan;

import org.syh.demo.simpledb.metadata.MetadataManager;
import org.syh.demo.simpledb.metadata.StatInfo;
import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.record.Layout;
import org.syh.demo.simpledb.query.TableScan;
import org.syh.demo.simpledb.record.Schema;
import org.syh.demo.simpledb.transaction.Transaction;

public class TablePlan implements Plan {
    private Transaction tx;
    private String tableName;
    private Layout layout;
    private StatInfo statInfo;

    public TablePlan(Transaction tx, String tableName, MetadataManager metadataManager) {
        this.tx = tx;
        this.tableName = tableName;
        this.layout = metadataManager.getLayout(tableName, tx);
        this.statInfo = metadataManager.getStatInfo(tableName, layout, tx);
    }

    @Override
    public Scan open() {
        return new TableScan(tx, tableName, layout);
    }

    @Override
    public int blocksAccessed() {
        return statInfo.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return statInfo.recordsOutput();
    }

    @Override
    public int distinctValues(String fldName) {
        return statInfo.distinctValues(fldName);
    }

    @Override
    public Schema schema() {
        return layout.schema();
    }
}
