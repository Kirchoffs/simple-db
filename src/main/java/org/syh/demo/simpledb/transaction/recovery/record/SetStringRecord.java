package org.syh.demo.simpledb.transaction.recovery.record;

import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.file.Page;
import org.syh.demo.simpledb.log.LogManager;
import org.syh.demo.simpledb.transaction.Transaction;

public class SetStringRecord implements LogRecord {
    private int txNum;
    private int offset;
    private String val;
    private BlockId blockId;

    public SetStringRecord(Page page) {
        int txNumPos = Integer.BYTES;
        this.txNum = page.getInt(txNumPos);

        int filenamePos = txNumPos + Integer.BYTES;
        String filename = page.getString(filenamePos);

        int blockNumPos = filenamePos + Page.maxLength(filename.length());
        int blockNum = page.getInt(blockNumPos);
        this.blockId = new BlockId(filename, blockNum);

        int offsetPos = blockNumPos + Integer.BYTES;
        this.offset = page.getInt(offsetPos);

        int valPos = offsetPos + Integer.BYTES;
        this.val = page.getString(valPos);
    }

    @Override
    public int op() {
        return SETSTRING;
    }

    @Override
    public int txNumber() {
        return txNum;
    }

    @Override
    public void undo(Transaction tx) {
        tx.pin(blockId);
        tx.setString(blockId, offset, val, false);
        tx.unpin(blockId);
    }

    public String toString() {
        return "<SETINT " + txNum + " " + blockId + " " + offset + " " + val + ">";
    }

    public static int writeToLog(LogManager logManager, int txNum, BlockId blockId, int offset, String val) {
        int txNumPos = Integer.BYTES;
        int filenamePos = txNumPos + Integer.BYTES;
        int blockNumPos = filenamePos + Page.maxLength(blockId.getFilename().length());
        int offsetPos = blockNumPos + Integer.BYTES;
        int valPos = offsetPos + Integer.BYTES;

        byte[] recordBytes = new byte[valPos + Integer.BYTES];
        Page page = new Page(recordBytes);
        page.setFirstInt(SETSTRING);
        page.setInt(txNumPos, txNum);
        page.setString(filenamePos, blockId.getFilename());
        page.setInt(blockNumPos, blockId.getBlockNum());
        page.setInt(offsetPos, offset);
        page.setString(valPos, val);

        return logManager.append(recordBytes);
    }
}
