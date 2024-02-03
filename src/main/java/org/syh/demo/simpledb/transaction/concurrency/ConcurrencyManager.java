package org.syh.demo.simpledb.transaction.concurrency;

import org.syh.demo.simpledb.file.BlockId;

import java.util.HashMap;
import java.util.Map;

public class ConcurrencyManager {
    private static LockTable lockTable = new LockTable();
    private Map<BlockId, LockType> locks = new HashMap<>(); // BlockId -> LockType (S or X)

    public void sLock(BlockId blockId) {
        if (!hasSLock(blockId)) {
            lockTable.sLock(blockId); // sLock is a synchronized method
            locks.put(blockId, LockType.S);
        }
    }

    public void release() {
        for (BlockId blockId : locks.keySet()) {
            lockTable.unlock(blockId);
        }
        locks.clear();
    }

    public void xLock(BlockId blockId) {
        if (!hasXLock(blockId)) {
            sLock(blockId);
            lockTable.xLock(blockId);
            locks.put(blockId, LockType.X);
        }
    }

    private boolean hasSLock(BlockId blockId) {
        return locks.containsKey(blockId);
    }

    private boolean hasXLock(BlockId blockId) {
        return locks.containsKey(blockId) && locks.get(blockId).equals(LockType.X);
    }

    private enum LockType {
        S, X
    }
}
