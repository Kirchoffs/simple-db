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
        // The lowest level directory page has flag as 0
        while (contents.getFlag() > 0) {
            BlockId childBlockId = findChildBlockId(searchKey);
            contents.close();
            contents = new BTreePage(tx, childBlockId, layout);
        }
        return findChildBlockId(searchKey).blockNum();
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

    private BlockId findChildBlockId(Constant searchKey) {
        int slot = contents.findSlotBefore(searchKey);
        int blockNum = contents.getBlockNum(slot);
        return new BlockId(fileName, blockNum);
    }
}
