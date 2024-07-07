package org.syh.demo.simpledb.metadata;

import org.syh.demo.simpledb.record.Layout;
import org.syh.demo.simpledb.record.Schema;
import org.syh.demo.simpledb.query.TableScan;
import org.syh.demo.simpledb.transaction.Transaction;

public class ViewManager {
    private static final int MAX_VIEW_DEF = 100;
    private TableManager tableManager;

    public ViewManager(boolean isNew, TableManager tableManager, Transaction tx) {
        this.tableManager = tableManager;
        if (isNew) {
            Schema schema = new Schema();
            schema.addStringField("viewName", TableManager.MAX_NAME_LENGTH);
            schema.addStringField("viewDef", MAX_VIEW_DEF);
            tableManager.createTable("viewCatalog", schema, tx);
        }
    }

    public void createView(String viewName, String viewDef, Transaction tx) {
        Layout layout = tableManager.getLayout("viewCatalog", tx);
        TableScan ts = new TableScan(tx, "viewCatalog", layout);
        ts.insert();
        ts.setString("viewName", viewName);
        ts.setString("viewDef", viewDef);
        ts.close();
    }

    public String getViewDef(String viewName, Transaction tx) {
        String res = null;
        Layout layout = tableManager.getLayout("viewCatalog", tx);
        TableScan ts = new TableScan(tx, "viewCatalog", layout);
        while (ts.next()) {
            if (ts.getString("viewName").equals(viewName)) {
                res = ts.getString("viewDef");
                break;
            }
        }
        ts.close();
        return res;
    }
}
