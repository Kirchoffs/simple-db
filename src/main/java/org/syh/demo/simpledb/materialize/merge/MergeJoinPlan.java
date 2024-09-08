package org.syh.demo.simpledb.materialize.merge;

import org.syh.demo.simpledb.materialize.sort.SortPlan;
import org.syh.demo.simpledb.materialize.sort.SortScan;
import org.syh.demo.simpledb.plan.Plan;
import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.record.Schema;
import org.syh.demo.simpledb.transaction.Transaction;

import java.util.Arrays;
import java.util.List;

public class MergeJoinPlan implements Plan {
    private Plan leftPlan, rightPlan;
    private String leftField, rightField;
    private Schema schema;

    public MergeJoinPlan(Transaction tx, Plan leftPlan, Plan rightPlan, String leftField, String rightField) {
        this.leftField = leftField;
        List<String> leftSortList = Arrays.asList(leftField);
        this.leftPlan = new SortPlan(tx, leftPlan, leftSortList);

        this.rightField = rightField;
        List<String> rightSortList = Arrays.asList(rightField);
        this.rightPlan = new SortPlan(tx, rightPlan, rightSortList);

        schema = new Schema();
        schema.addAll(leftPlan.schema());
        schema.addAll(rightPlan.schema());
    }

    @Override
    public Scan open() {
        Scan leftScan = leftPlan.open();
        SortScan rightScan = (SortScan) rightPlan.open();
        return new MergeJoinScan(leftScan, rightScan, leftField, rightField);
    }

    @Override
    public int blocksAccessed() {
        return leftPlan.blocksAccessed() + rightPlan.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        int maxDistinct = Math.max(leftPlan.distinctValues(leftField), rightPlan.distinctValues(rightField));
        return leftPlan.recordsOutput() * rightPlan.recordsOutput() / maxDistinct;
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
