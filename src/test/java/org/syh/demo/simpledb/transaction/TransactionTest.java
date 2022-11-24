package org.syh.demo.simpledb.transaction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.syh.demo.simpledb.CommonTestUtils;
import org.syh.demo.simpledb.buffer.BufferManager;
import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.file.FileManager;
import org.syh.demo.simpledb.log.LogManager;

import java.io.File;

import static org.syh.demo.simpledb.CommonTestConstants.BLOCK_SIZE;
import static org.syh.demo.simpledb.CommonTestConstants.BUFFER_POOL_SIZE;

public class TransactionTest {
    private FileManager fileManager;
    private LogManager logManager;
    private BufferManager bufferManager;

    private static final String DIR_PATH = "src/test/resources/concurrency-test/";
    private static final String LOG_FILE_NAME = "test-log-file";

    private File directory;

    @Before
    public void setup() {
        directory = new File(DIR_PATH);
        fileManager = new FileManager(directory, BLOCK_SIZE);
        logManager = new LogManager(fileManager, LOG_FILE_NAME);
        bufferManager = new BufferManager(fileManager, logManager, BUFFER_POOL_SIZE);

        CommonTestUtils.clearTestDir(directory);
    }

    @After
    public void cleanup() {
        CommonTestUtils.clearTestDir(directory);
    }

    @Test
    public void transactionTest() {
        Thread threadA = new Thread(new ThreadA());
        Thread threadB = new Thread(new ThreadB());
        Thread threadC = new Thread(new ThreadC());

        threadA.start();
        threadB.start();
        threadC.start();

        try {
            threadA.join();
            threadB.join();
            threadC.join();
        } catch (InterruptedException e) {}

        System.out.println("Done");
    }

    private class ThreadA implements Runnable {
        public void run() {
            try {
                Transaction txA = new Transaction(fileManager, logManager, bufferManager);
                BlockId blockId1 = new BlockId("test-file", 1);
                BlockId blockId2 = new BlockId("test-file", 2);
                txA.pin(blockId1); // It depends on the buffer memory pool size, and it will not be affected by slock or xlock.
                txA.pin(blockId2);
                System.out.println("Tx A: request sLock 1");
                txA.getInt(blockId1, 0);
                System.out.println("Tx A: receive sLock 1");
                Thread.sleep(1000);
                System.out.println("Tx A: request sLock 2");
                txA.getInt(blockId2, 0);
                System.out.println("Tx A: receive sLock 2");
                txA.commit();
                System.out.println("Tx A: commit");
            } catch (InterruptedException e) {}
        }
    }

    private class ThreadB implements Runnable {
        public void run() {
            try {
                Transaction txB = new Transaction(fileManager, logManager, bufferManager);
                BlockId blockId1 = new BlockId("test-file", 1);
                BlockId blockId2 = new BlockId("test-file", 2);
                txB.pin(blockId1);
                txB.pin(blockId2);
                System.out.println("Tx B: request xLock 2");
                txB.setInt(blockId2, 0, 0, false);
                System.out.println("Tx B: receive xLock 2");
                Thread.sleep(1000);
                System.out.println("Tx B: request sLock 1");
                txB.getInt(blockId1, 0);
                System.out.println("Tx B: receive sLock 1");
                txB.commit();
                System.out.println("Tx B: commit");
            } catch (InterruptedException e) {}
        }
    }

    private class ThreadC implements Runnable {
        public void run() {
            try {
                Transaction txC = new Transaction(fileManager, logManager, bufferManager);
                BlockId blockId1 = new BlockId("test-file", 1);
                BlockId blockId2 = new BlockId("test-file", 2);
                txC.pin(blockId1);
                txC.pin(blockId2);
                Thread.sleep(500);
                System.out.println("Tx C: request xLock 1");
                txC.setInt(blockId1, 0, 0, false);
                System.out.println("Tx C: receive xLock 1");
                Thread.sleep(1000);
                System.out.println("Tx C: request sLock 2");
                txC.getInt(blockId2, 0);
                System.out.println("Tx C: receive sLock 2");
                txC.commit();
                System.out.println("Tx C: commit");
            } catch (InterruptedException e) {}
        }
    }
}
