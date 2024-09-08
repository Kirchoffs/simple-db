package org.syh.demo.simpledb;

import java.io.File;

import static org.syh.demo.simpledb.CommonTestConstants.GIT_KEEP_FILE_NAME;

public class CommonTestUtils {
    public static void clearTestDir(File dir) {
        File[] files = dir.listFiles();

        if (files != null) {
            for (File file : files) {
                if (!file.getName().equals(GIT_KEEP_FILE_NAME)) {
                    file.delete();
                }
            }
        }
    }
}
