package org.syh.demo.simpledb.buffer;

import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.file.FileManager;
import org.syh.demo.simpledb.file.Page;
import org.syh.demo.simpledb.log.LogManager;

public class Buffer {
    private FileManager fileManager;
    private LogManager logManager;
    private Page contents;
    private BlockId blockId;
    private int pins;
    private int txNum;
    private int lsn;

    public Buffer(FileManager fileManager, LogManager logManager) {
        this.fileManager = fileManager;
        this.logManager = logManager;
        this.contents = new Page(fileManager.getBlockSize());
        this.pins = 0;
        this.txNum = -1;
        this.lsn = -1;
    }

    public Page getContents() {
        return contents;
    }

    public BlockId getBlockId() {
        return blockId;
    }

    public boolean isPinned() {
        return pins > 0;
    }

    public void pin() {
        pins++;
    }

    public void unpin() {
        pins--;
    }

    public void setModified(int txNum, int lsn) {
        this.txNum = txNum;
        if (lsn >= 0) {
            this.lsn = lsn;
        }
    }

    public int modifyingTx() {
        return txNum;
    }

    public void assignToBlock(BlockId blockId) {
        flush();
        this.blockId = blockId;
        fileManager.read(blockId, contents);
        pins = 0;
    }

    public void flush() {
        if (txNum >= 0) {
            logManager.flush(lsn);
            fileManager.write(blockId, contents);
            txNum = -1;
        }
    }
}
