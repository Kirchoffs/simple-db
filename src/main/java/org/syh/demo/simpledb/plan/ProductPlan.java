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
        schema.addAll(planLeft.schema());
        schema.addAll(planRight.schema());
    }

    @Override
    public Scan open() {
        return new ProductScan(planLeft.open(), planRight.open());
    }

    @Override
    public int blocksAccessed() {
        return planLeft.blocksAccessed() + planLeft.recordsOutput() * planRight.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return planLeft.recordsOutput() * planRight.recordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        if (planLeft.schema().hasField(fieldName)) {
            return planLeft.distinctValues(fieldName);
        } else {
            return planRight.distinctValues(fieldName);
        }
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
