package org.syh.demo.simpledb.plan.planner;

import org.syh.demo.simpledb.parse.data.CreateIndexData;
import org.syh.demo.simpledb.parse.data.CreateTableData;
import org.syh.demo.simpledb.parse.data.CreateViewData;
import org.syh.demo.simpledb.parse.data.DeleteData;
import org.syh.demo.simpledb.parse.data.InsertData;
import org.syh.demo.simpledb.parse.data.UpdateData;
import org.syh.demo.simpledb.transaction.Transaction;

public interface MutationPlanner {
    int executeInsert(InsertData data, Transaction tx);

    int executeDelete(DeleteData data, Transaction tx);

    int executeUpdate(UpdateData data, Transaction tx);

    int executeCreateTable(CreateTableData data, Transaction tx);

    int executeCreateView(CreateViewData data, Transaction tx);

    int executeCreateIndex(CreateIndexData data, Transaction tx);
}
