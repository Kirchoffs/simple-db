package org.syh.demo.simpledb.metadata;

import org.syh.demo.simpledb.record.Layout;
import org.syh.demo.simpledb.record.Schema;
import org.syh.demo.simpledb.query.TableScan;
import org.syh.demo.simpledb.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

import static org.syh.demo.simpledb.metadata.TableManager.MAX_NAME_LENGTH;

public class IndexManager {
    private Layout layout;
    private TableManager tableManager;
    private StatManager statManager;

    public IndexManager(boolean isNew, TableManager tableManager, StatManager statManager, Transaction tx) {
        if (isNew) {
            Schema schema = new Schema();
            schema.addStringField("indexName", MAX_NAME_LENGTH);
            schema.addStringField("tableName", MAX_NAME_LENGTH);
            schema.addStringField("fieldName", MAX_NAME_LENGTH);
            tableManager.createTable("indexCatalog", schema, tx);
        }

        this.tableManager = tableManager;
        this.statManager = statManager;
        layout = tableManager.getLayout("indexCatalog", tx);
    }

    public void createIndex(String indexName, String tableName, String fieldName, Transaction tx) {
        TableScan ts = new TableScan(tx, "indexCatalog", layout);
        ts.insert();
        ts.setString("indexName", indexName);
        ts.setString("tableName", tableName);
        ts.setString("fieldName", fieldName);
        ts.close();
    }

    public Map<String, IndexInfo> getIndexInfo(String tableName, Transaction tx) {
        Map<String, IndexInfo> res = new HashMap<>();
        TableScan ts = new TableScan(tx, "indexCatalog", layout);
        while (ts.next()) {
            if (ts.getString("tableName").equals(tableName)) {
                String indexName = ts.getString("indexName");
                String fieldName = ts.getString("fieldName");
                Layout tableLayout = tableManager.getLayout(tableName, tx);
                StatInfo tableStatInfo = statManager.getStatInfo(tableName, tableLayout, tx);
                IndexInfo indexInfo = new IndexInfo(indexName, fieldName, tableLayout.schema(), tx, tableStatInfo);
                res.put(fieldName, indexInfo);
            }
        }
        ts.close();
        return res;
    }
}
