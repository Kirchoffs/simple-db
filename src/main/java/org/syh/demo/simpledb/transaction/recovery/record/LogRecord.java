package org.syh.demo.simpledb.transaction.recovery.record;

import org.syh.demo.simpledb.file.Page;
import org.syh.demo.simpledb.transaction.Transaction;

public interface LogRecord {
    int CHECKPOINT = 0;
    int START = 1;
    int COMMIT = 2;
    int ROLLBACK  = 3;
    int SETINT = 4;
    int SETSTRING = 5;

    int op();

    int txNumber();

    void undo(Transaction tx);

    static LogRecord createLogRecord(byte[] bytes) {
        Page page = new Page(bytes); // Use Page class to easily decode and read the log record

        switch (page.getFirstInt()) {
            case CHECKPOINT:
                return new CheckpointRecord();
            case START:
                return new StartRecord(page);
            case COMMIT:
                return new CommitRecord(page);
            case ROLLBACK:
                return new RollbackRecord(page);
            case SETINT:
                return new SetIntRecord(page);
            case SETSTRING:
                return new SetStringRecord(page);
            default:
                return null;
        }
    }
}
