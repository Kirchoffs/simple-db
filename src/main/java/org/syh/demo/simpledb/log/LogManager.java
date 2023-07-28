package org.syh.demo.simpledb.log;

import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.file.FileManager;
import org.syh.demo.simpledb.file.Page;

import java.util.Iterator;

public class LogManager {
    private FileManager fileManager;
    private String logFile;
    private Page logPage;
    private BlockId logBlockId;
    private int latestLSN = 0; // LSN: Log Sequence Number
    private int lastSavedLSN = 0;

    public LogManager(FileManager fileManager, String logFile) {
        this.fileManager = fileManager;
        this.logFile = logFile;

        logPage = new Page(new byte[fileManager.getBlockSize()]);

        int logSize = fileManager.length(logFile);
        if (logSize == 0) {
            logBlockId = appendNewBlock();
        } else {
            logBlockId = new BlockId(logFile, logSize - 1);
            fileManager.read(logBlockId, logPage);
        }
    }

    public void flush(int lsn) {
        if (lsn >= lastSavedLSN) {
            flush();
        }
    }

    public synchronized int append(byte[] logRecord) {
        int boundary = logPage.getFirstInt();

        int recordSize = logRecord.length;
        int bytesNeeded = recordSize + Integer.BYTES;
        if (boundary - bytesNeeded < Integer.BYTES) {
            flush();
            logBlockId = appendNewBlock();
            boundary = logPage.getFirstInt();
        }

        int recordPosition = boundary - bytesNeeded;
        logPage.setBytes(recordPosition, logRecord);
        logPage.setFirstInt(recordPosition);

        latestLSN += 1;
        return latestLSN;
    }

    public Iterator<byte[]> iterator() {
        flush();
        return new LogIterator(fileManager, logBlockId);
    }

    private BlockId appendNewBlock() {
        BlockId blockId = fileManager.append(logFile);
        logPage.setFirstInt(fileManager.getBlockSize()); // Initial boundary value
        fileManager.write(blockId, logPage);
        return blockId;
    }

    private void flush() {
        fileManager.write(logBlockId, logPage);
        lastSavedLSN = latestLSN;
    }
}
