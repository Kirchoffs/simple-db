package org.syh.demo.simpledb.file;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class FileManagerTest {
    private static String DIR_PATH = "src/test/resources/file-manager-test/";
    private static String DATA_FILE_NAME = "test-data-file";
    private static int BLOCK_SIZE = 1024;
    private static int PAGE_SIZE = 1024;

    private File directory;

    @Before
    public void setup() {
        directory = new File(DIR_PATH);
        new File(directory, DATA_FILE_NAME).delete();
    }

    @After
    public void cleanup() {
        new File(directory, DATA_FILE_NAME).delete();
    }

    @Test
    public void fileManagerTest() {
        FileManager fileManager = new FileManager(directory, BLOCK_SIZE);

        BlockId blockId = fileManager.append(DATA_FILE_NAME);

        Page pageWrite = new Page(PAGE_SIZE);
        pageWrite.setInt(0, 42);
        pageWrite.setString(4, "xyz");
        fileManager.write(blockId, pageWrite);

        Page pageRead = new Page(PAGE_SIZE);
        fileManager.read(blockId, pageRead);
        Assert.assertEquals(42, pageRead.getInt(0));
        Assert.assertEquals("xyz", pageRead.getString(4));

        new File(directory, DATA_FILE_NAME).delete();
    }
}
