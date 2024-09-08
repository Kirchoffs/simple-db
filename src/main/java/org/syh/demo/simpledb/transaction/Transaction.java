package org.syh.demo.simpledb.transaction;

import org.syh.demo.simpledb.buffer.Buffer;
import org.syh.demo.simpledb.buffer.BufferManager;
import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.file.FileManager;
import org.syh.demo.simpledb.file.Page;
import org.syh.demo.simpledb.log.LogManager;
import org.syh.demo.simpledb.transaction.concurrency.ConcurrencyManager;
import org.syh.demo.simpledb.transaction.recovery.RecoveryManager;

/**
 * The class that provides transaction management for the database system.
 * Each transaction has its own recovery manager and concurrency manager.
 */
public class Transaction {
    private static int nextTxNum = 0;
    private static final int END_OF_FILE = -1;

    private RecoveryManager recoveryManager;
    private ConcurrencyManager concurrencyManager;
    private BufferManager bufferManager;
    private FileManager fileManager;

    private int txNum;
    private BufferList pinnedBuffers;

    public Transaction(FileManager fileManager, LogManager logManager, BufferManager bufferManager) {
        this.fileManager = fileManager;
        this.bufferManager = bufferManager;
        txNum = nextTxNumber();
        recoveryManager = new RecoveryManager(this, txNum, logManager, bufferManager); // Start a new transaction
        concurrencyManager = new ConcurrencyManager();
        pinnedBuffers = new BufferList(bufferManager);
    }

    private static synchronized int nextTxNumber() {
        nextTxNum++;
        return nextTxNum;
    }

    public void commit() {
        recoveryManager.commit();
        System.out.println("transaction " + txNum + " committed");
        concurrencyManager.release();
        pinnedBuffers.unpinAll();
    }

    public void rollback() {
        recoveryManager.rollback();
        System.out.println("transaction " + txNum + " rolled back");
        concurrencyManager.release();
        pinnedBuffers.unpinAll();
    }

    public void recover() {
        bufferManager.flushAll(txNum);
        recoveryManager.recover();
    }

    public void pin(BlockId blockId) {
        pinnedBuffers.pin(blockId);
    }

    public void unpin(BlockId blockId) {
        pinnedBuffers.unpin(blockId);
    }

    public int getInt(BlockId blockId, int offset) {
        concurrencyManager.sLock(blockId);
        Buffer buffer = pinnedBuffers.getBuffer(blockId);
        return buffer.contents().getInt(offset);
    }

    public int getFirstInt(BlockId blockId) {
        return getInt(blockId, 0);
    }

    public void setInt(BlockId blockId, int offset, int val, boolean okToLog) {
        concurrencyManager.xLock(blockId);
        Buffer buffer = pinnedBuffers.getBuffer(blockId);
        int lsn = -1;
        // WAL
        if (okToLog) {
            lsn = recoveryManager.setInt(buffer, offset, val);
        }

        Page page = buffer.contents();
        page.setInt(offset, val);
        buffer.setModified(txNum, lsn);
    }

    public void setFirstInt(BlockId blockId, int val, boolean okToLog) {
        setInt(blockId, 0, val, okToLog);
    }

    public String getString(BlockId blockId, int offset) {
        concurrencyManager.sLock(blockId);
        Buffer buffer = pinnedBuffers.getBuffer(blockId);
        return buffer.contents().getString(offset);
    }

    public void setString(BlockId blockId, int offset, String val, boolean okToLog) {
        concurrencyManager.xLock(blockId);
        Buffer buffer = pinnedBuffers.getBuffer(blockId);
        int lsn = -1;
        if (okToLog) {
            lsn = recoveryManager.setString(buffer, offset, val);
        }

        Page page = buffer.contents();
        page.setString(offset, val);
        buffer.setModified(txNum, lsn);
    }

    public int size(String fileName) {
        BlockId dummyBlockId = new BlockId(fileName, END_OF_FILE);
        concurrencyManager.sLock(dummyBlockId);
        return fileManager.length(fileName);
    }

    public BlockId append(String fileName) {
        BlockId blockId = new BlockId(fileName, END_OF_FILE);
        concurrencyManager.xLock(blockId);
        return fileManager.append(fileName);
    }

    public int getBlockSize() {
        return fileManager.blockSize();
    }

    public int getNumAvailable() {
        return bufferManager.numAvailable();
    }

    private static synchronized int nextTxNum() {
        nextTxNum++;
        return nextTxNum;
    }
}
