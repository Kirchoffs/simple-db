package org.syh.demo.simpledb.plan;

import org.syh.demo.simpledb.query.ProjectScan;
import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.record.Schema;

import java.util.List;

public class ProjectPlan implements Plan {
    private Plan plan;
    private Schema schema;

    public ProjectPlan(Plan plan, List<String> fieldList) {
        this.plan = plan;
        this.schema = new Schema();

        Schema originalSchema = plan.schema();
        for (String fieldName : fieldList) {
            schema.add(fieldName, originalSchema);
        }
    }

    @Override
    public Scan open() {
        Scan scan = plan.open();
        return new ProjectScan(scan, schema.fields());
    }

    @Override
    public int blocksAccessed() {
        return plan.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return plan.recordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        return plan.distinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
