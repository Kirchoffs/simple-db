package org.syh.demo.simpledb.index.planner;

import org.syh.demo.simpledb.index.Index;
import org.syh.demo.simpledb.index.query.IndexSelectScan;
import org.syh.demo.simpledb.metadata.IndexInfo;
import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.plan.Plan;
import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.query.TableScan;
import org.syh.demo.simpledb.record.Schema;

public class IndexSelectPlan implements Plan {
    private Plan plan;
    private IndexInfo indexInfo;
    private Constant val;

    public IndexSelectPlan(Plan plan, IndexInfo indexInfo, Constant val) {
       this.plan = plan;
       this.indexInfo = indexInfo;
       this.val = val;
    }

    @Override
    public Scan open() {
       TableScan tableScan = (TableScan) plan.open();
       Index index = indexInfo.open();
       return new IndexSelectScan(tableScan, index, val);
    }

    @Override
    public int blocksAccessed() {
       return indexInfo.getBlockAccessed() + recordsOutput();
    }

    @Override
    public int recordsOutput() {
       return indexInfo.getRecordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        return indexInfo.getDistinctValues(fieldName);
    }

    public Schema schema() {
        return plan.schema();
    }
}
