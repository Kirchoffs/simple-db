package org.syh.demo.simpledb.metadata;

import org.syh.demo.simpledb.record.Layout;
import org.syh.demo.simpledb.query.TableScan;
import org.syh.demo.simpledb.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

public class StatManager {
    private TableManager tableManager;
    private Map<String, StatInfo> tableStats;
    private int numCalls;

    public StatManager(TableManager tableManager, Transaction tx) {
        this.tableManager = tableManager;
        refreshStatistics(tx);
    }

    public synchronized StatInfo getStatInfo(String tableName, Layout layout, Transaction tx) {
        numCalls++;
        if (numCalls > 100) {
            refreshStatistics(tx);
        }

        StatInfo statInfo = tableStats.get(tableName);
        if (statInfo == null) {
            statInfo = calcTableStats(tableName, layout, tx);
            tableStats.put(tableName, statInfo);
        }

        return statInfo;
    }

    private synchronized void refreshStatistics(Transaction tx) {
        tableStats = new HashMap<>();
        numCalls = 0;

        Layout tableCatalogLayout = tableManager.getLayout("tableCatalog", tx);
        TableScan tableCatalogTs = new TableScan(tx, "tableCatalog", tableCatalogLayout);
        while (tableCatalogTs.next()) {
            String tableName = tableCatalogTs.getString("tableName");
            Layout layout = tableManager.getLayout(tableName, tx);
            StatInfo statInfo = calcTableStats(tableName, layout, tx);
            tableStats.put(tableName, statInfo);
        }

        tableCatalogTs.close();
    }

    private synchronized StatInfo calcTableStats(String tableName, Layout layout, Transaction tx) {
        int numRecords = 0;
        int numBlocks = 0;

        TableScan ts = new TableScan(tx, tableName, layout);
        while (ts.next()) {
            numRecords++;
            numBlocks = ts.getRid().getBlockNum() + 1;
        }
        ts.close();

        return new StatInfo(numBlocks, numRecords);
    }
}
