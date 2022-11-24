package org.syh.demo.simpledb.index;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.syh.demo.simpledb.buffer.BufferManager;
import org.syh.demo.simpledb.file.FileManager;
import org.syh.demo.simpledb.index.planner.IndexMutationPlanner;
import org.syh.demo.simpledb.log.LogManager;
import org.syh.demo.simpledb.metadata.MetadataManager;
import org.syh.demo.simpledb.metadata.TableManager;
import org.syh.demo.simpledb.parse.data.InsertData;
import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.query.TableScan;
import org.syh.demo.simpledb.record.Schema;
import org.syh.demo.simpledb.transaction.Transaction;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.syh.demo.simpledb.CommonTestConstants.BLOCK_SIZE;
import static org.syh.demo.simpledb.CommonTestConstants.BUFFER_POOL_SIZE;

public class BasicIndexTest {
    private static String DIR_PATH = "src/test/resources/index-test/";
    private static String LOG_FILE_NAME = "test-log-file";

    File directory = new File(DIR_PATH);

    @Test
    public void basicIndexTest() {
        FileManager fileManager = new FileManager(directory, BLOCK_SIZE);
        LogManager logManager = new LogManager(fileManager, LOG_FILE_NAME);
        BufferManager bufferManager = new BufferManager(fileManager, logManager, BUFFER_POOL_SIZE);

        Transaction tx = new Transaction(fileManager, logManager, bufferManager);
        MetadataManager metadataManager = new MetadataManager(true, tx);

        Schema vehicleSchema = new Schema();
        vehicleSchema.addStringField("year", TableManager.MAX_NAME_LENGTH);
        vehicleSchema.addStringField("make", TableManager.MAX_NAME_LENGTH);
        vehicleSchema.addStringField("model", TableManager.MAX_NAME_LENGTH);
        vehicleSchema.addIntField("id");
        metadataManager.createTable("vehicle", vehicleSchema, tx);

        metadataManager.createIndex("vehicle_year_idx", "vehicle", "year", tx);
        metadataManager.createIndex("vehicle_id_idx", "vehicle", "id", tx);

        IndexMutationPlanner indexMutationPlanner = new IndexMutationPlanner(metadataManager);
        List<InsertData> vehicleData = createVehicleData(4096);

        for (InsertData data : vehicleData) {
            indexMutationPlanner.executeInsert(data, tx);
        }

        tx.commit();

        TableScan tableScan = new TableScan(tx, "vehicle", metadataManager.getLayout("vehicle", tx));
        tableScan.beforeFirst();
        int count = 0;
        while (tableScan.next()) {
            String year = tableScan.getString("year");
            String make = tableScan.getString("make");
            String model = tableScan.getString("model");
            int id = tableScan.getInt("id");

            Assert.assertEquals("2021", year);
            Assert.assertEquals("Toyota", make);
            Assert.assertEquals("Corolla-" + id, model);
            Assert.assertEquals(count, id);
            count++;
        }
        Assert.assertEquals(4096, count);
    }

    private List<InsertData> createVehicleData(int N) {
        List<InsertData> insertDataList = new ArrayList<>();

        for (int i = 0; i < N; i++) {
            List<String> fieldNames = new ArrayList<>();
            List<Constant> values = new ArrayList<>();
            fieldNames.add("year");
            values.add(new Constant("2021"));
            fieldNames.add("make");
            values.add(new Constant("Toyota"));
            fieldNames.add("model");
            values.add(new Constant("Corolla-" + i));
            fieldNames.add("id");
            values.add(new Constant(i));
            insertDataList.add(new InsertData("vehicle", fieldNames, values));
        }

        return insertDataList;
    }

    @After
    public void tearDown() {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (!file.getName().equals(".gitkeep")) {
                    file.delete();
                }
            }
        }
    }
}
