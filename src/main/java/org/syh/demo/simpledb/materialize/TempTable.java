package org.syh.demo.simpledb.materialize;

import org.syh.demo.simpledb.query.TableScan;
import org.syh.demo.simpledb.query.UpdateScan;
import org.syh.demo.simpledb.record.Layout;
import org.syh.demo.simpledb.record.Schema;
import org.syh.demo.simpledb.transaction.Transaction;

public class TempTable {
    private static int nextTableNum = 0;
    private Transaction tx;
    private String tableName;
    private Layout layout;

    private static synchronized String nextTableName() {
        nextTableNum++;
        return "temp-" + nextTableNum;
    }

    public TempTable(Transaction tx, Schema schema) {
        this.tx = tx;
        tableName = nextTableName();
        this.layout = new Layout(schema);
    }

    public UpdateScan open() {
        return new TableScan(tx, tableName, layout);
    }

    public String tableName() {
        return tableName;
    }

    public Layout layout() {
        return layout;
    }
}
