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
    private int latestLSN = -1;   // LSN: Log Sequence Number, PostgreSQL has similar term.
                                  // LSM starts from 0, so the initial unset value is -1.
                                  // When append a new log record, the latestLSN will increase by 1 and be returned.
    private int lastSavedLSN = -1; // The latest LSN that has been saved to disk.

    public LogManager(FileManager fileManager, String logFile) {
        this.fileManager = fileManager;
        this.logFile = logFile;

        logPage = new Page(new byte[fileManager.blockSize()]);

        int logFileBlockSize = fileManager.length(logFile);
        if (logFileBlockSize == 0) {
            logBlockId = appendNewBlock();
        } else {
            logBlockId = new BlockId(logFile, logFileBlockSize - 1);
            fileManager.read(logBlockId, logPage);
        }
    }

    public void flush(int lsn) {
        if (lsn > lastSavedLSN) {
            flush();
        }
    }

    public synchronized int append(byte[] logRecord) {
        int boundary = logPage.getFirstInt();

        int recordSize = logRecord.length;
        int bytesNeeded = recordSize + Integer.BYTES; // content of logRecord + length of logRecord
        // The first byte is used to store the position of the upcoming log
        if (boundary - bytesNeeded < Integer.BYTES) {
            flush();
            logBlockId = appendNewBlock();
            boundary = logPage.getFirstInt();
        }

        int recordPosition = boundary - bytesNeeded;
        logPage.setBytes(recordPosition, logRecord); // It will write the length of the logRecord first, then the logRecord
        logPage.setFirstInt(recordPosition);

        latestLSN += 1;
        return latestLSN; // Return the lsn of the log record
    }

    public Iterator<byte[]> iterator() {
        flush();
        return new LogIterator(fileManager, logBlockId);
    }

    private BlockId appendNewBlock() {
        BlockId blockId = fileManager.append(logFile);
        logPage.setFirstInt(fileManager.blockSize()); // Initial boundary value
        fileManager.write(blockId, logPage);
        return blockId;
    }

    private void flush() {
        fileManager.write(logBlockId, logPage);
        lastSavedLSN = latestLSN;
    }
}
