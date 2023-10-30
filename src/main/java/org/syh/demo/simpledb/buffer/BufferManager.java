package org.syh.demo.simpledb.buffer;

import org.syh.demo.simpledb.buffer.exceptions.BufferAbortException;
import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.file.FileManager;
import org.syh.demo.simpledb.log.LogManager;

public class BufferManager {
    private int numAvailable;
    private Buffer[] bufferPool;
    private static final long MAX_TIME = 10000; // 10 seconds

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
        for (Buffer buffer: bufferPool) {
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
        for (Buffer buffer: bufferPool) {
            BlockId blockId = buffer.getBlockId();
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
            while (buffer == null && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);
                buffer = tryToPin(blockId);
            }

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
        for (Buffer buffer: bufferPool) {
            if (buffer.modifyingTx() == txNum) {
                buffer.flush();
            }
        }
    }

    private boolean waitingTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_TIME;
    }

    public synchronized int getNumAvailable() {
        return numAvailable;
    }
}
