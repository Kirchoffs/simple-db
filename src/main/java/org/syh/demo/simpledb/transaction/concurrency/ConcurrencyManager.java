package org.syh.demo.simpledb.transaction.concurrency;

import org.syh.demo.simpledb.file.BlockId;

import java.util.HashMap;
import java.util.Map;

public class ConcurrencyManager {
    private static LockTable globalLockTables = new LockTable();
    private Map<BlockId, LockType> localLocks = new HashMap<>(); // BlockId -> LockType (S or X)

    public void sLock(BlockId blockId) {
        if (!hasSLock(blockId)) {
            globalLockTables.sLock(blockId); // sLock is a synchronized method
            localLocks.put(blockId, LockType.S);
        }
    }

    public void xLock(BlockId blockId) {
        if (!hasXLock(blockId)) {
            sLock(blockId);
            globalLockTables.xLock(blockId);
            localLocks.put(blockId, LockType.X);
        }
    }

    public void release() {
        for (BlockId blockId : localLocks.keySet()) {
            globalLockTables.unlock(blockId);
        }
        localLocks.clear();
    }

    private boolean hasSLock(BlockId blockId) {
        return localLocks.containsKey(blockId);
    }

    private boolean hasXLock(BlockId blockId) {
        return localLocks.containsKey(blockId) && localLocks.get(blockId).equals(LockType.X);
    }

    private enum LockType {
        S, X
    }
}
