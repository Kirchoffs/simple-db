package org.syh.demo.simpledb.log;

import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.file.FileManager;
import org.syh.demo.simpledb.file.Page;

import java.util.Iterator;

public class LogIterator implements Iterator<byte[]> {
    private FileManager fileManager;
    private BlockId blockId;
    private Page page;
    private int currentPosition;
    private int boundary;

    public LogIterator(FileManager fileManager, BlockId blockId) {
        this.fileManager = fileManager;
        this.blockId = blockId;

        page = new Page(new byte[fileManager.blockSize()]);
        moveToBlock(blockId);
    }

    private void moveToBlock(BlockId blockId) {
        fileManager.read(blockId, page);
        boundary = page.getFirstInt();
        currentPosition = boundary;
    }

    @Override
    public boolean hasNext() {
        return currentPosition < fileManager.blockSize() || blockId.blockNum() > 0;
    }

    @Override
    public byte[] next() {
        if (currentPosition == fileManager.blockSize()) {
            if (blockId.blockNum() == 0) {
                return null;
            }

            blockId = new BlockId(blockId.fileName(), blockId.blockNum() - 1);
            moveToBlock(blockId);
        }

        byte[] record = page.getBytes(currentPosition);   // It will read the length of the logRecord first, then the logRecord
                                                          // The return value is just the logRecord without the length
        currentPosition += Integer.BYTES + record.length; // We need to add Integer.BYTES to skip the length
        return record;
    }
}
