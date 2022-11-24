package org.syh.demo.simpledb.index.query;

import org.junit.Test;
import org.syh.demo.simpledb.buffer.BufferManager;
import org.syh.demo.simpledb.file.FileManager;
import org.syh.demo.simpledb.log.LogManager;
import org.syh.demo.simpledb.transaction.Transaction;

import java.io.File;

import static org.syh.demo.simpledb.CommonTestConstants.BLOCK_SIZE;

public class IndexJoinScanTest {
    private static final String DIR_PATH = "src/test/resources/index-test/";
    private static final String LOG_FILE_NAME = "test-log-file";

    @Test
    public void indexJoinScanTest() {
        File directory = new File(DIR_PATH);

        FileManager fileManager = new FileManager(directory, BLOCK_SIZE);
        LogManager logManager = new LogManager(fileManager, LOG_FILE_NAME);
        BufferManager bufferManager = new BufferManager(fileManager, logManager, 8);

        Transaction tx = new Transaction(fileManager, logManager, bufferManager);
    }
}
