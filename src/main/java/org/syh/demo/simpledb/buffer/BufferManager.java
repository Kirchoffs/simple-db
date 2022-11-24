package org.syh.demo.simpledb.buffer;

import org.syh.demo.simpledb.buffer.exceptions.BufferAbortException;
import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.file.FileManager;
import org.syh.demo.simpledb.log.LogManager;

public class BufferManager {
    private int numAvailable;
    private Buffer[] bufferPool;
    private static final long MAX_WAIT_TIME = 10000; // 10 seconds

    public BufferManager(FileManager fileManager, LogManager logManager, int bufferPoolSize) {
        bufferPool = new Buffer[bufferPoolSize];
        for (int i = 0; i < bufferPoolSize; i++) {
            bufferPool[i] = new Buffer(fileManager, logManager);
        }
        numAvailable = bufferPoolSize;
    }

    public Buffer tryToPin(BlockId blockId) {
        Buffer buffer = findExistingBuffer(blockId);
        if (buffer == null) {
            buffer = chooseUnpinnedBuffer();
            if (buffer == null) {
                return null;
            }
            buffer.assignToBlock(blockId);
        }

        if (!buffer.isPinned()) {
            numAvailable--;
        }
        buffer.pin();

        return buffer;
    }

    private Buffer chooseUnpinnedBuffer() {
        for (Buffer buffer : bufferPool) {
            if (!buffer.isPinned()) {
                return buffer;
            }
        }
        return null;
    }

    /**
     * Find the existing buffer for the corresponding block
     * @param targetBlockId
     * @return
     */
    private Buffer findExistingBuffer(BlockId targetBlockId) {
        for (Buffer buffer : bufferPool) {
            BlockId blockId = buffer.blockId();
            if (blockId != null && blockId.equals(targetBlockId)) {
                return buffer;
            }
        }
        return null;
    }

    public synchronized Buffer pin(BlockId blockId) {
        try {
            long timestamp = System.currentTimeMillis();
            Buffer buffer = tryToPin(blockId);
            // Release the lock and wait for the buffer to be available
            // If it waits too long, it will break out of the loop and throw an exception.
            while (buffer == null && !waitingTooLong(timestamp)) {
                wait(MAX_WAIT_TIME); // It will change from TIME_WAITING to BLOCKED state when it is notified or the timeout period elapses.
                buffer = tryToPin(blockId);
            }
            // After the thread is woken up or the timeout period elapses,
            // it will attempt to re-acquire the lock.
            // If it cannot acquire the lock immediately, it will remain blocked and wait until the lock becomes available.
            // It will not proceed to the next statement until it successfully re-acquires the lock.

            // The CPU will not be occupied while the thread is waiting to re-acquire the lock.
            // The thread will be in a blocked state, and the CPU will be free to execute other tasks.
            // The thread will only consume CPU resources once it successfully re-acquires the lock and continues execution.

            if (buffer == null) {
                throw new BufferAbortException();
            }

            return buffer;
        } catch (InterruptedException exp) {
            throw new BufferAbortException();
        }
    }

    public synchronized void unpin(Buffer buffer) {
        buffer.unpin();
        if (!buffer.isPinned()) {
            numAvailable++;
            notifyAll();
        }
    }

    public synchronized void flushAll(int txNum) {
        for (Buffer buffer : bufferPool) {
            if (buffer.modifyingTx() == txNum) {
                buffer.flush();
            }
        }
    }

    private boolean waitingTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_WAIT_TIME;
    }

    public synchronized int numAvailable() {
        return numAvailable;
    }
}
