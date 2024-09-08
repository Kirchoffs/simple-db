package org.syh.demo.simpledb.transaction.recovery.record;

import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.file.Page;
import org.syh.demo.simpledb.log.LogManager;
import org.syh.demo.simpledb.transaction.Transaction;

public class SetIntRecord implements LogRecord {
    private int txNum;
    private int offset;
    private int val;
    private BlockId blockId;

    public SetIntRecord(Page page) {
        int txNumPos = Integer.BYTES;
        this.txNum = page.getInt(txNumPos);

        int fileNamePos = txNumPos + Integer.BYTES;
        String fileName = page.getString(fileNamePos);

        int blockNumPos = fileNamePos + Page.maxLength(fileName.length());
        int blockNum = page.getInt(blockNumPos);
        this.blockId = new BlockId(fileName, blockNum);

        int offsetPos = blockNumPos + Integer.BYTES;
        this.offset = page.getInt(offsetPos);

        int valPos = offsetPos + Integer.BYTES;
        this.val = page.getInt(valPos);
    }

    public int op() {
        return SETINT;
    }

    public int txNumber() {
        return txNum;
    }

    public void undo(Transaction tx) {
        tx.pin(blockId);
        tx.setInt(blockId, offset, val, false);
        tx.unpin(blockId);
    }

    public String toString() {
        return "<SETINT " + txNum + " " + blockId + " " + offset + " " + val + ">";
    }

    public static int writeToLog(LogManager logManager, int txNum, BlockId blockId, int offset, int val) {
        int txNumPos = Integer.BYTES;
        int fileNamePos = txNumPos + Integer.BYTES;
        int blockNumPos = fileNamePos + Page.maxLength(blockId.fileName().length());
        int offsetPos = blockNumPos + Integer.BYTES;
        int valPos = offsetPos + Integer.BYTES;

        byte[] recordBytes = new byte[valPos + Integer.BYTES];
        Page page = new Page(recordBytes);
        page.setFirstInt(SETINT);
        page.setInt(txNumPos, txNum);
        page.setString(fileNamePos, blockId.fileName());
        page.setInt(blockNumPos, blockId.blockNum());
        page.setInt(offsetPos, offset);
        page.setInt(valPos, val);

        return logManager.append(recordBytes);
    }
}
