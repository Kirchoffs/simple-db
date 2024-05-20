package org.syh.demo.simpledb.record;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.syh.demo.simpledb.buffer.BufferManager;
import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.file.FileManager;
import org.syh.demo.simpledb.log.LogManager;
import org.syh.demo.simpledb.transaction.Transaction;

import java.io.File;

public class TableScanTest {
    private static String DIR_PATH = "src/test/resources/record-test/";
    private static String DATA_FILE_NAME = "test-data-file";
    private static String LOG_FILE_NAME = "test-log-file";
    private static int BLOCK_SIZE = 1024;

    private File directory;

    @Before
    public void setup() {
        directory = new File(DIR_PATH);
        new File(directory, DATA_FILE_NAME).delete();
        new File(directory, LOG_FILE_NAME).delete();
    }

    @After
    public void cleanup() {
        new File(directory, DATA_FILE_NAME).delete();
        new File(directory, LOG_FILE_NAME).delete();
    }

    @Test
    public void tableScanTest() {
        FileManager fileManager = new FileManager(directory, BLOCK_SIZE);
        LogManager logManager = new LogManager(fileManager, LOG_FILE_NAME);
        BufferManager bufferManager = new BufferManager(fileManager, logManager, 3);

        Transaction tx = new Transaction(fileManager, logManager, bufferManager);

        Schema schema = new Schema();
        schema.addIntField("A");
        schema.addStringField("B", 9);
        Layout layout = new Layout(schema);
        for (String fieldName : layout.getSchema().fields()) {
            int offset = layout.getOffset(fieldName);
            System.out.println(fieldName + " has offset " + offset);
        }

        BlockId blockId = tx.append(DATA_FILE_NAME);
        tx.pin(blockId);
        TableScan ts = new TableScan(tx, DATA_FILE_NAME, layout);

        int N = 50;
        System.out.printf("Filling the table with %d random records...\n", N);
        for (int i = 0; i < N; i++) {
            ts.insert();
            int num = (int) Math.round(Math.random() * N);
            ts.setInt("A", num);
            ts.setString("B", "Record-" + num);
            System.out.printf("Inserting into %d-th slot %s: {%d, Record-%d}\n", i, ts.getRid(), num, num);
        }
        System.out.println();

        System.out.println("Deleting these records, whose X-values are less than 25...");
        int countDeleted = 0;
        ts.beforeFirst();
        while (ts.next()) {
            int a = ts.getInt("A");
            String b = ts.getString("B");
            if (a < 25) {
                countDeleted++;
                System.out.printf("Delete slot %s: {%d, %s}\n", ts.getRid(), a, b);
                ts.delete();
            }
        }
        System.out.println(countDeleted + " values under 25 were deleted.\n");

        System.out.println("Check the remaining records...");
        int countRemaining = 0;
        ts.beforeFirst();
        while (ts.next()) {
            int a = ts.getInt("A");
            String b = ts.getString("B");
            System.out.printf("Slot %s: {%d, %s}\n", ts.getRid(), a, b);
            countRemaining++;
        }
        System.out.println(countRemaining + " records are remaining.");
        ts.close();

        Assert.assertEquals(N, countDeleted + countRemaining);
    }
}
