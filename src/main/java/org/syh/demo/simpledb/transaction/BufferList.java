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
    private Map<BlockId, Buffer> buffers = new HashMap<>();
    private List<BlockId> pins = new ArrayList<>();

    public BufferList(BufferManager bufferManager) {
        this.bufferManager = bufferManager;
    }

    public Buffer getBuffer(BlockId blockId) {
        return buffers.get(blockId);
    }

    public void pin(BlockId blockId) {
        Buffer buffer = bufferManager.pin(blockId);
        buffers.put(blockId, buffer);
        pins.add(blockId);
    }

    public void unpin(BlockId blockId) {
        Buffer buffer = buffers.get(blockId);
        bufferManager.unpin(buffer);
        pins.remove(blockId);
        if (!pins.contains(blockId)) {
            buffers.remove(blockId);
        }
    }

    public void unpinAll() {
        for (BlockId blockId : pins) {
            Buffer buffer = buffers.get(blockId);
            bufferManager.unpin(buffer);
        }

        buffers.clear();
        pins.clear();
    }
}
