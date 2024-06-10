package org.syh.demo.simpledb.metadata;

import org.syh.demo.simpledb.record.Layout;
import org.syh.demo.simpledb.record.Schema;
import org.syh.demo.simpledb.transaction.Transaction;

import java.util.Map;

public class MetadataManager {
    private TableManager tableManager;
    private ViewManager viewManager;
    private StatManager statManager;
    private IndexManager indexManager;

    public MetadataManager(boolean isNew, Transaction tx) {
        tableManager = new TableManager(isNew, tx);
        viewManager = new ViewManager(isNew, tableManager, tx);
        statManager = new StatManager(tableManager, tx);
        indexManager = new IndexManager(isNew, tableManager, statManager, tx);
    }

    public void createTable(String tableName, Schema scheme, Transaction tx) {
        tableManager.createTable(tableName, scheme, tx);
    }

    public Layout getLayout(String tableName, Transaction tx) {
        return tableManager.getLayout(tableName, tx);
    }

    public void createView(String viewName, String viewDef, Transaction tx) {
        viewManager.createView(viewName, viewDef, tx);
    }

    public String getViewDef(String viewName, Transaction tx) {
        return viewManager.getViewDef(viewName, tx);
    }

    public void createIndex(String indexName, String tableName, String fieldName, Transaction tx) {
        indexManager.createIndex(indexName, tableName, fieldName, tx);
    }

    public Map<String, IndexInfo> getIndexInfo(String tableName, Transaction tx) {
        return indexManager.getIndexInfo(tableName, tx);
    }

    public StatInfo getStatInfo(String tableName, Layout layout, Transaction tx) {
        return statManager.getStatInfo(tableName, layout, tx);
    }
}
