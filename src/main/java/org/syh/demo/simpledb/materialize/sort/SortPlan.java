package org.syh.demo.simpledb.materialize.sort;

import org.syh.demo.simpledb.materialize.MaterializePlan;
import org.syh.demo.simpledb.materialize.TempTable;
import org.syh.demo.simpledb.plan.Plan;
import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.query.UpdateScan;
import org.syh.demo.simpledb.record.Schema;
import org.syh.demo.simpledb.transaction.Transaction;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class SortPlan implements Plan {
    private Transaction tx;
    private Plan plan;
    private Schema schema;
    private RecordComparator comparator;

    public SortPlan(Transaction tx, Plan plan, List<String> sortFieldNames) {
        this.tx = tx;
        this.plan = plan;
        schema = plan.schema();
        comparator = new RecordComparator(sortFieldNames);
    }

    @Override
    public Scan open() {
        Scan srcScan = plan.open();
        Queue<TempTable> runs = splitIntoRuns(srcScan);
        srcScan.close();

        while (runs.size() > 2) {
            runs = mergeIteration(runs);
        }

        return new SortScan(runs, comparator);
    }

    private Queue<TempTable> mergeIteration(Queue<TempTable> runs) {
        Queue<TempTable> res = new ArrayDeque<>();
        while (runs.size() > 1) {
            TempTable t1 = runs.poll();
            TempTable t2 = runs.poll();
            res.offer(mergeTwoRuns(t1, t2));
        }

        if (!runs.isEmpty()) {
            res.offer(runs.poll());
        }
        return res;
    }

    private TempTable mergeTwoRuns(TempTable alphaSrcTempTable, TempTable betaSrcTempTable) {
        Scan alphaSrcScan = alphaSrcTempTable.open();
        Scan betaSrcScan = betaSrcTempTable.open();
        TempTable dstTempTable = new TempTable(tx, schema);
        UpdateScan dstScan = dstTempTable.open();

        boolean hasMoreAlpha = alphaSrcScan.next();
        boolean hasMoreBeta = betaSrcScan.next();
        while (hasMoreAlpha && hasMoreBeta) {
            if (comparator.compare(alphaSrcScan, betaSrcScan) < 0) {
                hasMoreAlpha = copy(alphaSrcScan, dstScan);
            } else {
                hasMoreBeta = copy(betaSrcScan, dstScan);
            }
        }

        while (hasMoreAlpha) {
            hasMoreAlpha = copy(alphaSrcScan, dstScan);
        }
        while (hasMoreBeta) {
            hasMoreBeta = copy(betaSrcScan, dstScan);
        }

        alphaSrcScan.close();
        betaSrcScan.close();
        dstScan.close();

        return dstTempTable;
    }

    private Queue<TempTable> splitIntoRuns(Scan srcScan) {
        Queue<TempTable> tempTables = new ArrayDeque<>();
        srcScan.beforeFirst();

        if (!srcScan.next()) {
            return tempTables;
        }

        TempTable currentTempTable = new TempTable(tx, schema);
        tempTables.add(currentTempTable);
        UpdateScan currentTempScan = currentTempTable.open();
        while (copy(srcScan, currentTempScan)) {
            if (comparator.compare(srcScan, currentTempScan) < 0) {
                currentTempScan.close();
                currentTempTable = new TempTable(tx, schema);
                tempTables.add(currentTempTable);
                currentTempScan = currentTempTable.open();
            }
        }
        currentTempScan.close();

        return tempTables;
    }

    private boolean copy(Scan srcScan, UpdateScan dstScan) {
        dstScan.insert();
        for (String fieldName : schema.fields()) {
            dstScan.setVal(fieldName, srcScan.getVal(fieldName));
        }
        return srcScan.next();
    }

    @Override
    public int blocksAccessed() {
        Plan materializePlan = new MaterializePlan(plan, tx);
        return materializePlan.blocksAccessed();
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
