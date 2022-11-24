package org.syh.demo.simpledb.transaction;

import org.syh.demo.simpledb.buffer.Buffer;
import org.syh.demo.simpledb.buffer.BufferManager;
import org.syh.demo.simpledb.file.BlockId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manage the transaction's currently-pinned buffers.
 */
public class BufferList {
    private BufferManager bufferManager;
    private Map<BlockId, Buffer> pinnedBuffers = new HashMap<>();
    private Map<BlockId, Integer> pinnedCounts = new HashMap<>();

    public BufferList(BufferManager bufferManager) {
        this.bufferManager = bufferManager;
    }

    public Buffer getBuffer(BlockId blockId) {
        return pinnedBuffers.get(blockId);
    }

    public void pin(BlockId blockId) {
        Buffer buffer = bufferManager.pin(blockId);
        pinnedBuffers.put(blockId, buffer);
        pinnedCounts.put(blockId, pinnedCounts.getOrDefault(blockId, 0) + 1);
    }

    public void unpin(BlockId blockId) {
        Buffer buffer = pinnedBuffers.get(blockId);
        bufferManager.unpin(buffer);
        int count = pinnedCounts.get(blockId) - 1;
        if (count == 0) {
            pinnedBuffers.remove(blockId);
            pinnedCounts.remove(blockId);
        } else {
            pinnedCounts.put(blockId, count);
        }
    }

    public void unpinAll() {
        for (Map.Entry<BlockId, Buffer> entry : pinnedBuffers.entrySet()) {
            Buffer buffer = entry.getValue();
            int count = pinnedCounts.get(entry.getKey());
            for (int i = 0; i < count; i++) {
                bufferManager.unpin(buffer);
            }
        }

        pinnedBuffers.clear();
        pinnedCounts.clear();
    }
}
