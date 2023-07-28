package org.syh.demo.simplesb.file;

import org.junit.Assert;
import org.junit.Test;
import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.file.FileManager;
import org.syh.demo.simpledb.file.Page;

import java.io.File;

public class FileManagerTest {
    private static String DIR_PATH = "src/test/resources/file-manager-test/";
    private static String FILE_NAME = "test-db-file";
    private static int TEST_BLOCK_SIZE = 1024;
    private static int TEST_PAGE_SIZE = 1024;

    @Test
    public void fileManagerTest() throws Exception {
        File directory = new File(DIR_PATH);
        new File(directory, FILE_NAME).delete();

        FileManager fileManager = new FileManager(directory, TEST_BLOCK_SIZE);

        BlockId blockId = fileManager.append(FILE_NAME);

        Page pageWrite = new Page(TEST_PAGE_SIZE);
        pageWrite.setInt(0, 42);
        pageWrite.setString(4, "xyz");
        fileManager.write(blockId, pageWrite);

        Page pageRead = new Page(TEST_PAGE_SIZE);
        fileManager.read(blockId, pageRead);
        Assert.assertEquals(42, pageRead.getInt(0));
        Assert.assertEquals("xyz", pageRead.getString(4));

        new File(directory, FILE_NAME).delete();
    }
}
