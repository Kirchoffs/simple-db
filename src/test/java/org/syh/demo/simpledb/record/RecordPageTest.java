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

public class RecordPageTest {
    private static final String DIR_PATH = "src/test/resources/record-test/";
    private static final String DATA_FILE_NAME = "test-data-file";
    private static final String LOG_FILE_NAME = "test-log-file";
    private static final int BLOCK_SIZE = 1024;

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
    public void recordPageTest() {
        FileManager fileManager = new FileManager(directory, BLOCK_SIZE);
        LogManager logManager = new LogManager(fileManager, LOG_FILE_NAME);
        BufferManager bufferManager = new BufferManager(fileManager, logManager, 3);

        Transaction tx = new Transaction(fileManager, logManager, bufferManager);

        Schema schema = new Schema();
        schema.addIntField("X");
        schema.addStringField("Y", 42);
        Layout layout = new Layout(schema);
        for (String fieldName : layout.schema().fields()) {
            int offset = layout.getOffset(fieldName);
            System.out.println(fieldName + " has offset " + offset);
        }
        System.out.println();

        BlockId blockId = tx.append(DATA_FILE_NAME);
        tx.pin(blockId);

        RecordPage recordPage = new RecordPage(tx, blockId, layout);
        recordPage.format();
        int slot = -1;

        System.out.println("Check if the record page is formatted...");
        slot = recordPage.nextAfter(-1);
        int slotId = 0;
        while (slot != -1) {
            Assert.assertEquals(slotId, slot);
            Assert.assertTrue(recordPage.isEmpty(slot));
            System.out.println("Slot " + slot + " is empty: " + recordPage.isEmpty(slot));
            slot = recordPage.nextAfter(slot);
            slotId++;
        }
        System.out.println();

        System.out.println("Filling the record page with random records...");
        int count = 0;
        slot = recordPage.insertAfter(-1);
        while (slot != -1) {
            int num = (int) Math.round(Math.random() * 50);
            recordPage.setInt(slot, "X", num);
            recordPage.setString(slot, "Y", "Record-" + num);
            Assert.assertEquals(num, recordPage.getInt(slot, "X"));
            Assert.assertEquals("Record-" + num, recordPage.getString(slot, "Y"));
            System.out.println("Slot " + slot + " is filled: " + recordPage.getInt(slot, "X") + ", " + recordPage.getString(slot, "Y"));
            slot = recordPage.insertAfter(slot);
            count++;
        }
        System.out.println("Total " + count + " records are filled.\n");

        System.out.println("Deleting these records, whose X-values are less than 25...");
        int countDeleted = 0;
        slot = recordPage.nextAfter(-1);
        while (slot != -1) {
            int x = recordPage.getInt(slot, "X");
            String y = recordPage.getString(slot, "Y");
            if (x < 25) {
                countDeleted++;
                System.out.println("Slot " + slot + ": {" + x + ", " + y + "}");
                recordPage.delete(slot);
            }
            slot = recordPage.nextAfter(slot);
        }
        System.out.println(countDeleted + " records with value under 25 are deleted.\n");

        System.out.println("Check the remaining records...");
        int countRemaining = 0;
        slot = recordPage.nextAfter(-1);
        while (slot != -1) {
            int x = recordPage.getInt(slot, "X");
            String y = recordPage.getString(slot, "Y");
            System.out.println("Slot " + slot + ": {" + x + ", " + y + "}");
            slot = recordPage.nextAfter(slot);
            countRemaining++;
        }
        System.out.println(countRemaining + " records are remaining.");

        Assert.assertEquals(count, countDeleted + countRemaining);
    }
}
