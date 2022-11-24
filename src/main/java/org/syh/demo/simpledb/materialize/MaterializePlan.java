package org.syh.demo.simpledb.materialize;

import org.syh.demo.simpledb.plan.Plan;
import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.query.UpdateScan;
import org.syh.demo.simpledb.record.Layout;
import org.syh.demo.simpledb.record.Schema;
import org.syh.demo.simpledb.transaction.Transaction;

public class MaterializePlan implements Plan {
    private Plan srcPlan;
    private Transaction tx;

    public MaterializePlan(Plan srcPlan, Transaction tx) {
        this.srcPlan = srcPlan;
        this.tx = tx;
    }

    @Override
    public Scan open() {
        Schema schema = srcPlan.schema();
        TempTable tempTable = new TempTable(tx, schema);
        Scan srcScan = srcPlan.open();
        UpdateScan tempScan = tempTable.open();
        while (srcScan.next()) {
            tempScan.insert();
            for (String fieldName : schema.fields()) {
                tempScan.setVal(fieldName, srcScan.getVal(fieldName));
            }
        }
        srcScan.close();
        tempScan.beforeFirst();
        return tempScan;
    }

    @Override
    public int blocksAccessed() {
        Layout layout = new Layout(srcPlan.schema());
        double rpb = (double) tx.getBlockSize() / layout.slotSize();
        return (int) Math.ceil(srcPlan.recordsOutput() / rpb);
    }

    @Override
    public int recordsOutput() {
        return srcPlan.recordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        return srcPlan.distinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return srcPlan.schema();
    }
}
