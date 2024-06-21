package org.syh.demo.simpledb.metadata;

import org.junit.After;
import org.junit.Test;
import org.syh.demo.simpledb.buffer.BufferManager;
import org.syh.demo.simpledb.file.FileManager;
import org.syh.demo.simpledb.log.LogManager;
import org.syh.demo.simpledb.record.FieldType;
import org.syh.demo.simpledb.record.Layout;
import org.syh.demo.simpledb.record.Schema;
import org.syh.demo.simpledb.record.TableScan;
import org.syh.demo.simpledb.transaction.Transaction;

import java.io.File;
import java.util.Map;



public class MetadataManagerTest {
    private static String DIR_PATH = "src/test/resources/metadata-manager-test/";
    private static String LOG_FILE_NAME = "test-log-file";
    private static int BLOCK_SIZE = 1024;
    private static int BUFFER_POOL_SIZE = 3;

    private File directory = new File(DIR_PATH);

    @Test
    public void metadataManagerTest() {
        FileManager fileManager = new FileManager(directory, BLOCK_SIZE);
        LogManager logManager = new LogManager(fileManager, LOG_FILE_NAME);
        BufferManager bufferManager = new BufferManager(fileManager, logManager, BUFFER_POOL_SIZE);

        Transaction tx = new Transaction(fileManager, logManager, bufferManager);
        MetadataManager metadataManager = new MetadataManager(true, tx);

        Schema schema = new Schema();
        schema.addStringField("year", TableManager.MAX_NAME_LENGTH);
        schema.addStringField("make", TableManager.MAX_NAME_LENGTH);
        schema.addStringField("model", TableManager.MAX_NAME_LENGTH);
        schema.addIntField("price");
        metadataManager.createTable("vehicle", schema, tx);

        // Table Metadata
        Layout vehicleLayout = metadataManager.getLayout("vehicle", tx);
        int vehicleSlotSize = vehicleLayout.getSlotSize();
        Schema vehicleSchema = vehicleLayout.getSchema();
        System.out.println("Table vehicle has slot size " + vehicleSlotSize);
        System.out.println("Its fields are:");
        for (String fieldName : vehicleSchema.fields()) {
            String type;
            if (vehicleSchema.type(fieldName) == FieldType.INTEGER) {
                type = "int";
            } else {
                int length = vehicleSchema.length(fieldName);
                type = "varchar(" + length + ")";
            }
            System.out.println(fieldName + ": " + type);
        }

        // Statistics Metadata
        TableScan vehicleTs = new TableScan(tx, "vehicle", vehicleLayout);
        for (int i = 0; i < 50; i++) {
            vehicleTs.insert();
            vehicleTs.setString("year", "2021");
            vehicleTs.setString("make", "Toyota");
            vehicleTs.setString("model", "Corolla" + i);
            vehicleTs.setInt("price", 20000 + i);
        }
        StatInfo vehicleStatInfo = metadataManager.getStatInfo("vehicle", vehicleLayout, tx);
        System.out.println("B(vehicle) = " + vehicleStatInfo.blocksAccessed());
        System.out.println("R(vehicle) = " + vehicleStatInfo.recordsOutput());
        System.out.println("V(vehicle, year) = " + vehicleStatInfo.distinctValues("year"));
        System.out.println("V(vehicle, make) = " + vehicleStatInfo.distinctValues("make"));
        System.out.println("V(vehicle, model) = " + vehicleStatInfo.distinctValues("model"));
        System.out.println("V(vehicle, price) = " + vehicleStatInfo.distinctValues("price"));

        // View Metadata
        String viewDefInput = "select model from vehicle where price > 20010";
        metadataManager.createView("viewVehicle", viewDefInput, tx);
        String viewDefOutput = metadataManager.getViewDef("viewVehicle", tx);
        System.out.println("View def = " + viewDefOutput);

        // Index Metadata
        metadataManager.createIndex("indexVehicleModel", "vehicle", "model", tx);
        metadataManager.createIndex("indexVehiclePrice", "vehicle", "price", tx);
        Map<String, IndexInfo> indexInfoMap = metadataManager.getIndexInfo("vehicle", tx);

        IndexInfo modelIndexInfo = indexInfoMap.get("model");
        System.out.println("B(indexVehicleModel) = " + modelIndexInfo.blockAccessed());

        IndexInfo priceIndexInfo = indexInfoMap.get("price");
        System.out.println("B(indexVehiclePrice) = " + priceIndexInfo.blockAccessed());

        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.getName().startsWith("test") || file.getName().endsWith(".tbl")) {
                    file.delete();
                }
            }
        }
    }

    @After
    public void tearDown() {
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
