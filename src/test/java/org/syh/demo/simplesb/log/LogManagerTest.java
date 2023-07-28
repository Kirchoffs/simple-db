package org.syh.demo.simplesb.log;

import org.junit.Test;
import org.syh.demo.simpledb.file.FileManager;
import org.syh.demo.simpledb.file.Page;
import org.syh.demo.simpledb.log.LogManager;

import java.io.File;
import java.util.Iterator;

public class LogManagerTest {
    private static String DIR_PATH = "src/test/resources/log-manager-test/";
    private static String FILE_NAME = "test-log-file";
    private static int TEST_BLOCK_SIZE = 256;

    @Test
    public void LogManagerTest() {
        File directory = new File(DIR_PATH);
        new File(directory, FILE_NAME).delete();

        FileManager fileManager = new FileManager(directory, TEST_BLOCK_SIZE);
        LogManager logManager = new LogManager(fileManager, "test-log-file");

        printLogRecords(logManager, "The initial empty log file: ");

        createRecords(logManager, 1, 35);
        printLogRecords(logManager, "The log file now has these records: ");

        createRecords(logManager, 36, 70);
        logManager.flush(65);
        printLogRecords(logManager, "The log file now has these records: ");

        new File(directory, FILE_NAME).delete();
    }

    private void printLogRecords(LogManager logManager, String msg) {
        System.out.println(msg);

        Iterator<byte[]> iter = logManager.iterator();
        while (iter.hasNext()) {
            byte[] record = iter.next();
            Page page = new Page(record);

            String str = page.getString(0);
            int num = page.getInt(Page.maxLength(str.length()));

            System.out.println("[" + str + ", " + num + "]");
        }

        System.out.println();
    }

    private void createRecords(LogManager logManager, int start, int end) {
        System.out.print("Creating records: ");

        for (int i = start; i <= end; i++) {
            byte[] record = createLogRecord("record-" + i, i + 100);
            int lsn = logManager.append(record);
            System.out.print(lsn + " ");
        }

        System.out.println();
    }

    private static byte[] createLogRecord(String str, int num) {
        int sPos = 0;
        int nPos = sPos + Page.maxLength(str.length());

        byte[] bytes = new byte[Page.maxLength(str.length()) + Integer.BYTES];
        Page page = new Page(bytes);
        page.setString(sPos, str);
        page.setInt(nPos, num);

        return bytes;
    }
}
