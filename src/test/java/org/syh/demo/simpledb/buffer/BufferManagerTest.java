package org.syh.demo.simpledb.buffer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.syh.demo.simpledb.buffer.exceptions.BufferAbortException;
import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.file.FileManager;
import org.syh.demo.simpledb.log.LogManager;

import java.io.File;

public class BufferManagerTest {
    private static final String DIR_PATH = "src/test/resources/buffer-test/";
    private static final  String DATA_FILE_NAME = "test-data-file";
    private static final  String LOG_FILE_NAME = "test-log-file";
    private static final int BLOCK_SIZE = 1024;
    private static final int BUFFER_POOL_SIZE = 3;

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
    public void bufferManagerTest() {
        FileManager fileManager = new FileManager(directory, BLOCK_SIZE);
        LogManager logManager = new LogManager(fileManager, LOG_FILE_NAME);
        BufferManager bufferManager = new BufferManager(fileManager, logManager, BUFFER_POOL_SIZE);

        Buffer[] buffers = new Buffer[6];
        buffers[0] = bufferManager.pin(new BlockId(DATA_FILE_NAME, 0));
        buffers[1] = bufferManager.pin(new BlockId(DATA_FILE_NAME, 1));
        buffers[2] = bufferManager.pin(new BlockId(DATA_FILE_NAME, 2));
        bufferManager.unpin(buffers[1]); buffers[1] = null;
        buffers[3] = bufferManager.pin(new BlockId(DATA_FILE_NAME, 0));    // block 0 pinned twice
        buffers[4] = bufferManager.pin(new BlockId(DATA_FILE_NAME, 1));    // block 1 re-pinned

        System.out.println("Available buffers: " + bufferManager.numAvailable());
        Assert.assertEquals(0, bufferManager.numAvailable());

        try {
            System.out.println("Attempting to pin block 3...");
            buffers[5] = bufferManager.pin(new BlockId(DATA_FILE_NAME, 3)); // will not work since no buffers left
            Assert.fail();
        } catch(BufferAbortException e) {
            System.out.println("Exception: No available buffers");
            System.out.println();
        }

        bufferManager.unpin(buffers[2]); buffers[2] = null;
        buffers[5] = bufferManager.pin(new BlockId(DATA_FILE_NAME, 3));

        System.out.println("Final Buffer Allocation: ");
        for (int i = 0; i < buffers.length; i++) {
            Buffer buffer = buffers[i];
            if (buffer != null) {
                System.out.println("buff[" + i + "] pinned to block " + buffer.blockId());
            }
        }
    }
}
