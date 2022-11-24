package org.syh.demo.simpledb.plan.planner;

import org.syh.demo.simpledb.metadata.MetadataManager;
import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.parse.data.CreateIndexData;
import org.syh.demo.simpledb.parse.data.CreateTableData;
import org.syh.demo.simpledb.parse.data.CreateViewData;
import org.syh.demo.simpledb.parse.data.DeleteData;
import org.syh.demo.simpledb.parse.data.InsertData;
import org.syh.demo.simpledb.parse.data.UpdateData;
import org.syh.demo.simpledb.plan.Plan;
import org.syh.demo.simpledb.plan.SelectPlan;
import org.syh.demo.simpledb.plan.TablePlan;
import org.syh.demo.simpledb.query.UpdateScan;
import org.syh.demo.simpledb.transaction.Transaction;
import org.syh.demo.simpledb.utils.Pair;

import java.util.Iterator;

public class BasicMutationPlanner implements MutationPlanner {
    private MetadataManager metadataManager;

    public BasicMutationPlanner(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    public int executeInsert(InsertData data, Transaction tx) {
        Plan plan = new TablePlan(tx, data.tableName(), metadataManager);

        UpdateScan updateScan = (UpdateScan) plan.open();
        updateScan.insert();

        Iterator<Pair<String, Constant>> iter = data.fields().iterator();
        while (iter.hasNext()) {
            Pair<String, Constant> field = iter.next();
            updateScan.setVal(field.getFirst(), field.getSecond());
        }

        updateScan.close();

        return 1;
    }

    public int executeDelete(DeleteData data, Transaction tx) {
        Plan plan = new TablePlan(tx, data.tableName(), metadataManager);
        plan = new SelectPlan(plan, data.predicate());

        UpdateScan updateScan = (UpdateScan) plan.open();

        int count = 0;
        while (updateScan.next()) {
            updateScan.delete();
            count++;
        }

        updateScan.close();

        return count;
    }

    public int executeUpdate(UpdateData data, Transaction tx) {
        Plan plan = new TablePlan(tx, data.tableName(), metadataManager);
        plan = new SelectPlan(plan, data.predicate());

        UpdateScan updateScan = (UpdateScan) plan.open();

        int count = 0;
        while (updateScan.next()) {
            Constant val = data.value().evaluate(updateScan);
            updateScan.setVal(data.fieldName(), val);
            count++;
        }

        updateScan.close();

        return count;
    }

    public int executeCreateTable(CreateTableData data, Transaction tx) {
        metadataManager.createTable(data.tableName(), data.schema(), tx);
        return 0;
    }

    public int executeCreateView(CreateViewData data, Transaction tx) {
        metadataManager.createView(data.viewName(), data.viewDef(), tx);
        return 0;
    }

    public int executeCreateIndex(CreateIndexData data, Transaction tx) {
        metadataManager.createIndex(data.indexName(), data.tableName(), data.fieldName(), tx);
        return 0;
    }
}
