package org.syh.demo.simpledb.metadata;

import org.junit.Test;
import org.syh.demo.simpledb.buffer.BufferManager;
import org.syh.demo.simpledb.file.FileManager;
import org.syh.demo.simpledb.log.LogManager;
import org.syh.demo.simpledb.record.Layout;
import org.syh.demo.simpledb.record.Schema;
import org.syh.demo.simpledb.query.TableScan;
import org.syh.demo.simpledb.transaction.Transaction;

import java.io.File;

public class CatalogTest {
    private static String DIR_PATH = "src/test/resources/catalog-test/";
    private static String LOG_FILE_NAME = "test-log-file";
    private static int BLOCK_SIZE = 1024;
    private static int BUFFER_POOL_SIZE = 3;

    @Test
    public void catalogTest() {
        File directory = new File(DIR_PATH);

        FileManager fileManager = new FileManager(directory, BLOCK_SIZE);
        LogManager logManager = new LogManager(fileManager, LOG_FILE_NAME);
        BufferManager bufferManager = new BufferManager(fileManager, logManager, BUFFER_POOL_SIZE);

        Transaction tx = new Transaction(fileManager, logManager, bufferManager);
        TableManager tableManager = new TableManager(true, tx);
        Layout tableCatalogLayout = tableManager.getLayout("tableCatalog", tx);

        Schema vehicleSchema = new Schema();
        vehicleSchema.addStringField("year", TableManager.MAX_NAME_LENGTH);
        vehicleSchema.addStringField("make", TableManager.MAX_NAME_LENGTH);
        vehicleSchema.addStringField("model", TableManager.MAX_NAME_LENGTH);
        tableManager.createTable("vehicle", vehicleSchema, tx);

        System.out.println("Here are all the tables and their lengths.");
        TableScan tableCatalogTs = new TableScan(tx, "tableCatalog", tableCatalogLayout);
        while (tableCatalogTs.next()) {
            String tableName = tableCatalogTs.getString("tableName");
            int slotSize = tableCatalogTs.getInt("slotSize");
            System.out.println(tableName + " " + slotSize);
        }
        tableCatalogTs.close();
        System.out.println();

        System.out.println("Here are the fields for each table and their offsets");
        Layout fieldCatalogLayout = tableManager.getLayout("fieldCatalog", tx);
        TableScan fieldCatalogTs = new TableScan(tx, "fieldCatalog", fieldCatalogLayout);
        while (fieldCatalogTs.next()) {
            String tableName = fieldCatalogTs.getString("tableName");
            String fieldName = fieldCatalogTs.getString("fieldName");
            int offset = fieldCatalogTs.getInt("offset");
            System.out.println(tableName + " " + fieldName + " " + offset);
        }

        tx.commit();

        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.getName().startsWith("test") || file.getName().endsWith(".tbl")) {
                    file.delete();
                }
            }
        }
    }
}
