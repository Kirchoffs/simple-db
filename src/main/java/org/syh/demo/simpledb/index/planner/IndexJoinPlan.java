package org.syh.demo.simpledb.index.planner;

import org.syh.demo.simpledb.index.Index;
import org.syh.demo.simpledb.index.query.IndexJoinScan;
import org.syh.demo.simpledb.metadata.IndexInfo;
import org.syh.demo.simpledb.plan.Plan;
import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.query.TableScan;
import org.syh.demo.simpledb.record.Schema;

public class IndexJoinPlan implements Plan {
    private Plan leftPlan;
    private Plan rightPlan;
    private IndexInfo rightIndexInfo;
    private String joinField;
    private Schema schema;

    public IndexJoinPlan(Plan leftPlan, Plan rightPlan, IndexInfo rightIndexInfo, String joinField) {
        this.leftPlan = leftPlan;
        this.rightPlan = rightPlan;
        this.rightIndexInfo = rightIndexInfo;
        this.joinField = joinField;
        schema = new Schema();
        schema.addAll(leftPlan.schema());
        schema.addAll(rightPlan.schema());
    }

    @Override
    public Scan open() {
        Scan leftScan = leftPlan.open();
        TableScan rightTableScan = (TableScan) rightPlan.open();
        Index rightIndex = rightIndexInfo.open();
        return new IndexJoinScan(leftScan, rightIndex, joinField, rightTableScan);
    }

    @Override
    public int blocksAccessed() {
        return leftPlan.blocksAccessed() + leftPlan.recordsOutput() * rightIndexInfo.getBlockAccessed() + recordsOutput();
    }

    @Override
    public int recordsOutput() {
        return leftPlan.recordsOutput() * rightIndexInfo.getRecordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        if (leftPlan.schema().hasField(fieldName)) {
            return leftPlan.distinctValues(fieldName);
        } else {
            return rightPlan.distinctValues(fieldName);
        }
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
