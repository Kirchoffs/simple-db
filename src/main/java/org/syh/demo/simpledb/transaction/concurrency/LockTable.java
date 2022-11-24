package org.syh.demo.simpledb.transaction.concurrency;

import org.syh.demo.simpledb.file.BlockId;

import java.util.HashMap;
import java.util.Map;

public class LockTable {
    private static final long MAX_WAIT_TIME = 10000;

    // If the value is positive, it means the number of SLocks.
    // If the value is -1, it means there is an XLock.
    private Map<BlockId, Integer> locks = new HashMap<>();

    public synchronized void sLock(BlockId blockId) {
        try {
            long timestamp = System.currentTimeMillis();
            while (hasXLock(blockId) && !waitingTooLong(timestamp)) {
                wait(MAX_WAIT_TIME);
            }
            if (hasXLock(blockId)) {
                throw new LockAbortException();
            }
            int val = getLockVal(blockId);
            locks.put(blockId, val + 1);
        } catch (InterruptedException e) {
            throw new LockAbortException();
        }
    }

    /**
     * Before trying to get an XLock, we must first get an SLock.
     */
    public synchronized void xLock(BlockId blockId) {
        try {
            long timestamp = System.currentTimeMillis();
            while (hasOtherSLocks(blockId) && !waitingTooLong(timestamp)) {
                wait(MAX_WAIT_TIME);
            }
            if (hasOtherSLocks(blockId)) {
                throw new LockAbortException();
            }
            locks.put(blockId, -1);
        } catch (InterruptedException e) {
            throw new LockAbortException();
        }
    }

    public synchronized void unlock(BlockId blockId) {
        int val = getLockVal(blockId);
        if (val > 1) {
            if (val == 2) {
                notifyAll();
            }
            locks.put(blockId, val - 1);
        } else {
            locks.remove(blockId);
            notifyAll();
        }
    }

    private boolean hasXLock(BlockId blockId) {
        return getLockVal(blockId) < 0;
    }

    private boolean waitingTooLong(long timestamp) {
        return System.currentTimeMillis() - timestamp > MAX_WAIT_TIME;
    }

    private boolean hasOtherSLocks(BlockId blockId) {
        return getLockVal(blockId) > 1;
    }

    private int getLockVal(BlockId blockId) {
        return locks.getOrDefault(blockId, 0);
    }
}
