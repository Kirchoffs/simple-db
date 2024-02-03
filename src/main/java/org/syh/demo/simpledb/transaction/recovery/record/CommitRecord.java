package org.syh.demo.simpledb.transaction.recovery.record;

import org.syh.demo.simpledb.file.Page;
import org.syh.demo.simpledb.log.LogManager;
import org.syh.demo.simpledb.transaction.Transaction;

public class CommitRecord implements LogRecord {
    private int txNum;

    public CommitRecord(Page page) {
        int txNumPos = Integer.BYTES;
        this.txNum = page.getInt(txNumPos);
    }

    public int op() {
        return COMMIT;
    }

    public int txNumber() {
        return txNum;
    }

    public void undo(Transaction tx) {}

    public String toString() {
        return "<COMMIT " + txNum + ">";
    }

    public static int  writeToLog(LogManager logManager, int txNum) {
        byte[] record = new byte[Integer.BYTES * 2];
        Page page = new Page(record);
        page.setFirstInt(COMMIT);
        page.setInt(Integer.BYTES, txNum);
        return logManager.append(record);
    }
}
