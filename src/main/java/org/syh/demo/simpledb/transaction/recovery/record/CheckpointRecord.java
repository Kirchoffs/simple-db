package org.syh.demo.simpledb.transaction.recovery.record;

import org.syh.demo.simpledb.file.Page;
import org.syh.demo.simpledb.log.LogManager;
import org.syh.demo.simpledb.transaction.Transaction;

public class CheckpointRecord implements LogRecord {
    public CheckpointRecord() {}

    public int op() {
        return CHECKPOINT;
    }

    // Checkpoint records have no associated transaction
    public int txNumber() {
        return -1;
    }

    public void undo(Transaction tx) {}

    public String toString() {
        return "<CHECKPOINT>";
    }

    public static int writeToLog(LogManager logManager) {
        byte[] record = new byte[Integer.BYTES];
        Page page = new Page(record);
        page.setFirstInt(CHECKPOINT);
        return logManager.append(record);
    }
}
