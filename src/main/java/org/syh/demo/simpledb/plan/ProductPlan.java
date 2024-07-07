package org.syh.demo.simpledb.plan;

import org.syh.demo.simpledb.query.ProductScan;
import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.record.Schema;

public class ProductPlan implements Plan {
    private Plan planLeft;
    private Plan planRight;
    private Schema schema;

    public ProductPlan(Plan planLeft, Plan planRight) {
        this.planLeft = planLeft;
        this.planRight = planRight;
        schema = new Schema();
        schema.addAll(planLeft.getSchema());
        schema.addAll(planRight.getSchema());
    }

    public Scan open() {
        return new ProductScan(planLeft.open(), planRight.open());
    }

    public int blocksAccessed() {
        return planLeft.blocksAccessed() + planLeft.recordsOutput() * planRight.blocksAccessed();
    }

    public int recordsOutput() {
        return planLeft.recordsOutput() * planRight.recordsOutput();
    }

    public int distinctValues(String fieldName) {
        if (planLeft.getSchema().hasField(fieldName)) {
            return planLeft.distinctValues(fieldName);
        } else {
            return planRight.distinctValues(fieldName);
        }
    }

    public Schema getSchema() {
        return schema;
    }
}
