package org.syh.demo.simpledb.buffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.file.FileManager;
import org.syh.demo.simpledb.file.Page;
import org.syh.demo.simpledb.log.LogManager;

import java.io.File;

public class BufferTest {
    private static final String DIR_PATH = "src/test/resources/buffer-test/";
    private static final String DATA_FILE_NAME = "test-data-file";
    private static final String LOG_FILE_NAME = "test-log-file";
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
    public void bufferTest() {
        FileManager fileManager = new FileManager(directory, BLOCK_SIZE);
        LogManager logManager = new LogManager(fileManager, LOG_FILE_NAME);
        BufferManager bufferManager = new BufferManager(fileManager, logManager, BUFFER_POOL_SIZE);

        Buffer buffer1 = bufferManager.pin(new BlockId(DATA_FILE_NAME, 1));
        Page page1 = buffer1.contents();
        int num = page1.getInt(80);
        page1.setInt(80, num + 1);
        buffer1.setModified(1, 0);
        System.out.println("The new value is " + (num + 1));
        bufferManager.unpin(buffer1);

        Buffer buffer2 = bufferManager.pin(new BlockId(DATA_FILE_NAME, 2));
        Buffer buffer3 = bufferManager.pin(new BlockId(DATA_FILE_NAME, 3));
        Buffer buffer4 = bufferManager.pin(new BlockId(DATA_FILE_NAME, 4));

        bufferManager.unpin(buffer2);
        buffer2 = bufferManager.pin(new BlockId(DATA_FILE_NAME, 1));
        Page page2 = buffer2.contents();
        page2.setInt(80, 9999);
        buffer2.setModified(1, 0);
    }
}
