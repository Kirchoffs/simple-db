package org.syh.demo.simpledb.transaction.recovery;

import org.syh.demo.simpledb.buffer.Buffer;
import org.syh.demo.simpledb.buffer.BufferManager;
import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.log.LogManager;
import org.syh.demo.simpledb.transaction.Transaction;
import org.syh.demo.simpledb.transaction.recovery.record.CommitRecord;
import org.syh.demo.simpledb.transaction.recovery.record.LogRecord;
import org.syh.demo.simpledb.transaction.recovery.record.RollbackRecord;
import org.syh.demo.simpledb.transaction.recovery.record.SetIntRecord;
import org.syh.demo.simpledb.transaction.recovery.record.SetStringRecord;
import org.syh.demo.simpledb.transaction.recovery.record.StartRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import static org.syh.demo.simpledb.transaction.recovery.record.LogRecord.CHECKPOINT;
import static org.syh.demo.simpledb.transaction.recovery.record.LogRecord.COMMIT;
import static org.syh.demo.simpledb.transaction.recovery.record.LogRecord.ROLLBACK;
import static org.syh.demo.simpledb.transaction.recovery.record.LogRecord.START;

/**
 * The recovery manager.
 * Each transaction has its own recovery manager.
 */
public class RecoveryManager {
    private LogManager logManager;
    private BufferManager bufferManager;
    private Transaction tx;
    private int txNum;

    public RecoveryManager(Transaction tx, int txNum, LogManager logManager, BufferManager bufferManager) {
        this.tx = tx;
        this.txNum = txNum;
        this.logManager = logManager;
        this.bufferManager = bufferManager;
        StartRecord.writeToLog(logManager, txNum);
    }

    /**
     * Write a commit record to the log, and flushes it to disk.
     */
    public void commit() {
        // After flush all buffers to disk, then write the commit record.
        // This is undo-only logging.
        bufferManager.flushAll(txNum);
        int lsn = CommitRecord.writeToLog(logManager, txNum);
        logManager.flush(lsn);
    }

    /**
     * Write a rollback record to the log and flush it to disk.
     */
    public void rollback() {
        doRollback();
        bufferManager.flushAll(txNum);
        int lsn = RollbackRecord.writeToLog(logManager, txNum);
        logManager.flush(lsn);
    }

    /**
     * Recover uncompleted transactions from the log
     * and then write a quiescent checkpoint record to the log and flush it.
     */
    public void recover() {
        doRecover();
        bufferManager.flushAll(txNum);
        int lsn = CommitRecord.writeToLog(logManager, txNum);
        logManager.flush(lsn);
    }

    private void doRollback() {
        Iterator<byte[]> iter = logManager.iterator();
        while (iter.hasNext()) {
            byte[] bytes = iter.next();
            LogRecord record = LogRecord.createLogRecord(bytes);
            if (record.txNumber() == txNum) {
                if (record.op() == START) {
                    return;
                }
                record.undo(tx);
            }
        }
    }

    private void doRecover() {
        Collection<Integer> finishedTxs = new ArrayList<>();
        Iterator<byte[]> iter = logManager.iterator();
        while (iter.hasNext()) {
            byte[] bytes = iter.next();
            LogRecord record = LogRecord.createLogRecord(bytes);
            if (record.op() == CHECKPOINT) {
                return;
            }

            if (record.op() == COMMIT || record.op() == ROLLBACK) {
                finishedTxs.add(record.txNumber());
            } else if (!finishedTxs.contains(record.txNumber())) {
                record.undo(tx);
            }
        }
    }

    /**
     * Write a setInt record to the log and return its lsn.
     */
    public int setInt(Buffer buffer, int offset, int newVal) {
        int oldVal = buffer.getContents().getInt(offset);
        BlockId blockId = buffer.getBlockId();
        return SetIntRecord.writeToLog(logManager, txNum, blockId, offset, oldVal);
    }

    public int setString(Buffer buffer, int offset, String newVal) {
        String oldVal = buffer.getContents().getString(offset);
        BlockId blockId = buffer.getBlockId();
        return SetStringRecord.writeToLog(logManager, txNum, blockId, offset, oldVal);
    }
}
