package org.syh.demo.simpledb.metadata;

import org.syh.demo.simpledb.metadata.exceptions.MetaDataCorruptedException;
import org.syh.demo.simpledb.record.FieldType;
import org.syh.demo.simpledb.record.Layout;
import org.syh.demo.simpledb.record.Schema;
import org.syh.demo.simpledb.record.TableScan;
import org.syh.demo.simpledb.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

public class TableManager {
    public static final int MAX_NAME_LENGTH = 42;

    private Layout tableCatalogLayout;
    private Layout fieldCatalogLayout;

    public TableManager(boolean isNew, Transaction tx) {
        Schema tableCatalogSchema = new Schema();
        tableCatalogSchema.addStringField("tableName", MAX_NAME_LENGTH);
        tableCatalogSchema.addIntField("slotSize");
        tableCatalogLayout = new Layout(tableCatalogSchema);

        Schema fieldCatalogSchema = new Schema();
        fieldCatalogSchema.addStringField("tableName", MAX_NAME_LENGTH);
        fieldCatalogSchema.addStringField("fieldName", MAX_NAME_LENGTH);
        fieldCatalogSchema.addIntField("type");
        fieldCatalogSchema.addIntField("length");
        fieldCatalogSchema.addIntField("offset");
        fieldCatalogLayout = new Layout(fieldCatalogSchema);

        if (isNew) {
            createTable("tableCatalog", tableCatalogSchema, tx);
            createTable("fieldCatalog", fieldCatalogSchema, tx);
        }
    }

    public void createTable(String tableName, Schema schema, Transaction tx) {
        Layout layout = new Layout(schema);

        TableScan tableCatalogTS = new TableScan(tx, "tableCatalog", tableCatalogLayout);
        tableCatalogTS.insert();
        tableCatalogTS.setString("tableName", tableName);
        tableCatalogTS.setInt("slotSize", layout.getSlotSize());
        tableCatalogTS.close();

        TableScan fieldCatalogTS = new TableScan(tx, "fieldCatalog", fieldCatalogLayout);
        for (String fieldName : schema.fields()) {
            fieldCatalogTS.insert();
            fieldCatalogTS.setString("tableName", tableName);
            fieldCatalogTS.setString("fieldName", fieldName);
            fieldCatalogTS.setInt("type", schema.type(fieldName).getValue());
            fieldCatalogTS.setInt("length", schema.length(fieldName));
            fieldCatalogTS.setInt("offset", layout.getOffset(fieldName));
        }
        fieldCatalogTS.close();
    }

    public Layout getLayout(String tableName, Transaction tx) {
        int size = -1;
        TableScan tableCatalogTs = new TableScan(tx, "tableCatalog", tableCatalogLayout);
        while (tableCatalogTs.next()) {
            if (tableCatalogTs.getString("tableName").equals(tableName)) {
                size = tableCatalogTs.getInt("slotSize");
                break;
            }
        }
        tableCatalogTs.close();

        Schema schema = new Schema();
        Map<String, Integer> offsets = new HashMap<>();
        TableScan fieldCatalogTs = new TableScan(tx, "fieldCatalog", fieldCatalogLayout);
        while (fieldCatalogTs.next()) {
            if (fieldCatalogTs.getString("tableName").equals(tableName)) {
                String fieldName = fieldCatalogTs.getString("fieldName");
                FieldType type = FieldType.fromValue(fieldCatalogTs.getInt("type"));
                if (type == null) {
                    throw new MetaDataCorruptedException();
                }
                int length = fieldCatalogTs.getInt("length");
                int offset = fieldCatalogTs.getInt("offset");
                schema.addField(fieldName, type, length);
                offsets.put(fieldName, offset);
            }
        }
        fieldCatalogTs.close();

        return new Layout(schema, offsets, size);
    }
}
