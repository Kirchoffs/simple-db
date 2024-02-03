package org.syh.demo.simpledb.transaction.recovery.record;

import org.syh.demo.simpledb.file.Page;
import org.syh.demo.simpledb.log.LogManager;
import org.syh.demo.simpledb.transaction.Transaction;

public class RollbackRecord implements LogRecord {
    private int txNum;
    public RollbackRecord(Page page) {
        int txNumPos = Integer.BYTES;
        this.txNum = page.getInt(txNumPos);
    }

    public int op() {
        return ROLLBACK;
    }

    public int txNumber() {
        return txNum;
    }

    public void undo(Transaction tx) {}

    public String toString() {
        return "<ROLLBACK " + txNum + ">";
    }

    public static int writeToLog(LogManager logManager, int txNum) {
        byte[] record = new byte[Integer.BYTES * 2];
        Page page = new Page(record);
        page.setFirstInt(ROLLBACK);
        page.setInt(Integer.BYTES, txNum);
        return logManager.append(record);
    }
}
