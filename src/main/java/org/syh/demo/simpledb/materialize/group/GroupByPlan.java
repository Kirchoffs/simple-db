package org.syh.demo.simpledb.materialize.group;

import org.syh.demo.simpledb.materialize.sort.SortPlan;
import org.syh.demo.simpledb.plan.Plan;
import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.record.Schema;
import org.syh.demo.simpledb.transaction.Transaction;

import java.util.List;

public class GroupByPlan implements Plan {
    private Plan plan;
    private List<String> groupFields;
    private List<AggregationFn> aggregationFns;
    private Schema schema;

    public GroupByPlan(Transaction tx, Plan plan, List<String> groupFields, List<AggregationFn> aggregationFns) {
        this.plan = new SortPlan(tx, plan, groupFields);
        this.groupFields = groupFields;
        this.aggregationFns = aggregationFns;

        schema = new Schema();
        for (String fieldName : groupFields) {
            schema.add(fieldName, plan.schema());
        }
        for (AggregationFn aggregationFn : aggregationFns) {
            schema.addIntField(aggregationFn.fieldName());
        }
    }

    @Override
    public Scan open() {
        Scan scan = plan.open();
        return new GroupByScan(scan, groupFields, aggregationFns);
    }

    @Override
    public int blocksAccessed() {
        return plan.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        int numGroups = 1;
        for (String fieldName : groupFields) {
            numGroups *= plan.distinctValues(fieldName);
        }
        return numGroups;
    }

    @Override
    public int distinctValues(String fieldName) {
        if (groupFields.contains(fieldName)) {
            return plan.distinctValues(fieldName);
        } else {
            return recordsOutput();
        }
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
