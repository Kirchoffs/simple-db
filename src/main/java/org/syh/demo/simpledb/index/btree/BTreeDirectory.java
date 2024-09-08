package org.syh.demo.simpledb.index.btree;

import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.record.Layout;
import org.syh.demo.simpledb.transaction.Transaction;

public class BTreeDirectory {
    private Transaction tx;
    private Layout layout;
    private BTreePage contents;
    private String fileName;

    public BTreeDirectory(Transaction tx, BlockId blockId, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        contents = new BTreePage(tx, blockId, layout);
        fileName = blockId.fileName();
    }

    public void close() {
        contents.close();
    }

    public int search(Constant searchKey) {
        // Find the child block whose dataVal is smaller than or equal to the searchKey
        // If smaller, the last
        // If equal, the first
        BlockId childBlockId = findChildBlockId(searchKey);
        while (contents.getFlag() > 0) {
            contents.close();
            // In the last iteration, contents will be the lowest level directory page
            contents = new BTreePage(tx, childBlockId, layout);
            // In the last iteration, we found the entry (pointer to leaf page) on the lowest level directory page
            childBlockId = findChildBlockId(searchKey);
        }
        return childBlockId.blockNum();
    }

    public void makeNewRoot(DirectoryEntry entry) {
        Constant firstVal = contents.getDataVal(0);
        int level = contents.getFlag();
        BlockId newBlockId = contents.split(0, level);
        DirectoryEntry oldRootEntry = new DirectoryEntry(firstVal, newBlockId.blockNum());
        insertEntry(oldRootEntry);
        insertEntry(entry);
        contents.setFlag(level + 1);
    }

    public DirectoryEntry insert(DirectoryEntry entry) {
        if (contents.getFlag() == 0) {
            return insertEntry(entry);
        } else {
            BlockId childBlockId = findChildBlockId(entry.dataVal());
            BTreeDirectory childDirectory = new BTreeDirectory(tx, childBlockId, layout);
            DirectoryEntry potentialNewEntry = childDirectory.insert(entry);
            childDirectory.close();
            return (potentialNewEntry == null ? null : insertEntry(potentialNewEntry));
        }
    }

    private DirectoryEntry insertEntry(DirectoryEntry entry) {
        // The first slot whose dataVal is greater than or equal to the new entry
        int newSlot = 1 + contents.findSlotBefore(entry.dataVal());
        contents.insertDirectoryData(newSlot, entry.dataVal(), entry.blockNumber());
        if (!contents.isFull()) {
            return null;
        } else {
            int level = contents.getFlag();
            int splitPos = contents.getNumRecords() / 2;
            Constant splitVal = contents.getDataVal(splitPos);
            BlockId newBlockId = contents.split(splitPos, level);
            // For a directory entry, the dataVal is the first dataVal of its corresponding block
            return new DirectoryEntry(splitVal, newBlockId.blockNum());
        }
    }

    // Find the last child block whose dataVal is smaller than the searchKey
    // Or find the first child block whose dataVal is equal to the searchKey
    private BlockId findChildBlockId(Constant searchKey) {
        // Find the last slot whose dataVal is smaller than the searchKey
        int slot = contents.findSlotBefore(searchKey);
        if (contents.getDataVal(slot + 1).equals(searchKey)) {
            slot++;
        }
        int blockNum = contents.getBlockNum(slot);
        return new BlockId(fileName, blockNum);
    }
}
