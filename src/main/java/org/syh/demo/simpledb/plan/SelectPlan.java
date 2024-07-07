package org.syh.demo.simpledb.plan;

import org.syh.demo.simpledb.parse.Predicate;
import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.query.SelectScan;
import org.syh.demo.simpledb.record.Schema;

public class SelectPlan implements Plan {
    private Plan plan;
    private Predicate predicate;

    public SelectPlan(Plan plan, Predicate predicate) {
        this.plan = plan;
        this.predicate = predicate;
    }

    public Scan open() {
        Scan scan = plan.open();
        return new SelectScan(scan, predicate);
    }

    public int blocksAccessed() {
        return plan.blocksAccessed();
    }

    public int recordsOutput() {
        return plan.recordsOutput() / predicate.reductionFactor(plan);
    }

    public int distinctValues(String fieldName) {
        if (predicate.equatesWithConstant(fieldName) != null) {
            return 1;
        } else {
            String otherFieldName = predicate.equatesWithField(fieldName);
            if (otherFieldName != null) {
                return Math.min(plan.distinctValues(fieldName), plan.distinctValues(otherFieldName));
            } else {
                return plan.distinctValues(fieldName);
            }
        }
    }

    public Schema getSchema() {
        return plan.getSchema();
    }
}
