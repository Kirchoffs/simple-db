package org.syh.demo.simpledb.index.planner;

import org.syh.demo.simpledb.index.Index;
import org.syh.demo.simpledb.metadata.IndexInfo;
import org.syh.demo.simpledb.metadata.MetadataManager;
import org.syh.demo.simpledb.parse.data.CreateIndexData;
import org.syh.demo.simpledb.parse.data.CreateTableData;
import org.syh.demo.simpledb.parse.data.CreateViewData;
import org.syh.demo.simpledb.parse.data.DeleteData;
import org.syh.demo.simpledb.parse.data.InsertData;
import org.syh.demo.simpledb.parse.data.UpdateData;
import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.plan.Plan;
import org.syh.demo.simpledb.plan.SelectPlan;
import org.syh.demo.simpledb.plan.TablePlan;
import org.syh.demo.simpledb.plan.planner.MutationPlanner;
import org.syh.demo.simpledb.query.UpdateScan;
import org.syh.demo.simpledb.record.Rid;
import org.syh.demo.simpledb.transaction.Transaction;
import org.syh.demo.simpledb.utils.Pair;

import java.util.Iterator;
import java.util.Map;

public class IndexMutationPlanner implements MutationPlanner {
    private MetadataManager metadataManager;

    public IndexMutationPlanner(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    @Override
    public int executeInsert(InsertData data, Transaction tx) {
        String tableName = data.tableName();
        Plan plan = new TablePlan(tx, tableName, metadataManager);

        UpdateScan updateScan = (UpdateScan) plan.open();
        updateScan.insert();
        Rid rid = updateScan.getRid();

        Map<String, IndexInfo> indexes = metadataManager.getIndexInfo(tableName, tx);
        for (Pair<String, Constant> field : data.fields()) {
            String fieldName = field.getFirst();
            Constant value = field.getSecond();
            updateScan.setVal(fieldName, value);

            IndexInfo indexInfo = indexes.get(fieldName);
            if (indexInfo != null) {
                Index index = indexInfo.open();
                index.insert(value, rid);
                index.close();
            }
        }

        updateScan.close();
        return 1;
    }

    @Override
    public int executeDelete(DeleteData data, Transaction tx) {
        String tableName = data.tableName();
        Plan plan = new TablePlan(tx, tableName, metadataManager);
        plan = new SelectPlan(plan, data.predicate());
        Map<String, IndexInfo> indexes = metadataManager.getIndexInfo(tableName, tx);

        UpdateScan updateScan = (UpdateScan) plan.open();
        int count = 0;
        while(updateScan.next()) {
            Rid rid = updateScan.getRid();
            for (Map.Entry<String, IndexInfo> indexEntry : indexes.entrySet()) {
                String fieldName = indexEntry.getKey();
                IndexInfo indexInfo = indexEntry.getValue();
                Index index = indexInfo.open();
                index.delete(updateScan.getVal(fieldName), rid);
                index.close();
            }
            updateScan.delete();
            count++;
        }

        updateScan.close();
        return count;
    }

    @Override
    public int executeUpdate(UpdateData data, Transaction tx) {
        String tableName = data.tableName();
        String fieldName = data.fieldName();
        Plan plan = new TablePlan(tx, tableName, metadataManager);
        plan = new SelectPlan(plan, data.predicate());

        IndexInfo indexInfo = metadataManager.getIndexInfo(tableName, tx).get(fieldName);
        Index index = (indexInfo == null) ? null : indexInfo.open();

        UpdateScan updateScan = (UpdateScan) plan.open();
        int count = 0;
        while(updateScan.next()) {
            Constant newValue = data.value().evaluate(updateScan);
            Constant oldValue = updateScan.getVal(fieldName);
            updateScan.setVal(fieldName, newValue);

            if (index != null) {
                Rid rid = updateScan.getRid();
                index.delete(oldValue, rid);
                index.insert(newValue, rid);
            }
        }

        return count;
    }

    @Override
    public int executeCreateTable(CreateTableData data, Transaction tx) {
        metadataManager.createTable(data.tableName(), data.schema(), tx);
        return 0;
    }

    @Override
    public int executeCreateView(CreateViewData data, Transaction tx) {
        metadataManager.createView(data.viewName(), data.viewDef(), tx);
        return 0;
    }

    @Override
    public int executeCreateIndex(CreateIndexData data, Transaction tx) {
        metadataManager.createIndex(data.indexName(), data.tableName(), data.fieldName(), tx);
        return 0;
    }
}
