package org.syh.demo.simpledb.index.btree;

import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.record.Layout;
import org.syh.demo.simpledb.record.Rid;
import org.syh.demo.simpledb.transaction.Transaction;

public class BTreeLeaf {
    private Transaction tx;
    private Layout layout;
    private Constant searchKey;
    private BTreePage contents;
    private int currentSlot;
    private String fileName;

    public BTreeLeaf(Transaction tx, BlockId blockId, Layout layout, Constant searchKey) {
        this.tx = tx;
        this.layout = layout;
        this.searchKey = searchKey;
        contents = new BTreePage(tx, blockId, layout);
        currentSlot = contents.findSlotBefore(searchKey);
        fileName = blockId.fileName();
    }

    public void close() {
        contents.close();
    }

    public boolean next() {
        currentSlot++;
        if (currentSlot >= contents.getNumRecords()) {
            return tryOverflow();
        } else if (contents.getDataVal(currentSlot).equals(searchKey)) {
            return true;
        } else {
            return tryOverflow(); // If the first key of the block is the same as the search key, then we can move to the overflow block
        }
    }

    public Rid getDataRid() {
        return contents.getDataRid(currentSlot);
    }

    public void delete(Rid dataRid) {
        while (next()) {
            if (getDataRid().equals(dataRid)) {
                contents.delete(currentSlot);
                return;
            }
        }
    }

    public DirectoryEntry insert(Rid dataRid) {
        // According to constructor code:
        //   contents = new BTreePage(tx, blockId, layout);
        //   currentSlot = contents.findSlotBefore(searchKey);
        // searchKey is used to determine the value of currentSlot
        //
        // It is possible that searchKey is less than the first value in the leaf page
        // In this case, currentSlot will be -1
        //
        // When searching for index block, we use the dataVal of a directory entry to compare with searchKey
        // It is possible the dataVal of the directory entry (more specifically, the first entry of index block 0) is smaller than the searchKey
        // but the dataVal of the block pointed by the directory entry is greater than the searchKey
        if (contents.getFlag() >= 0 && contents.getDataVal(0).compareTo(searchKey) > 0) {
            // If its flag is not -1, it has an overflow block, and all elements of the overflow block are the same.
            Constant firstVal = contents.getDataVal(0);
            BlockId newBlockId = contents.split(0, contents.getFlag()); // Its actually not splitting but moving all elements to a new block
            currentSlot = 0;
            contents.setFlag(-1);
            contents.insertLeafData(currentSlot, searchKey, dataRid);
            return new DirectoryEntry(firstVal, newBlockId.blockNum());
        }

        currentSlot++;
        contents.insertLeafData(currentSlot, searchKey, dataRid);
        if (!contents.isFull()) {
            return null;
        }
        Constant firstKey = contents.getDataVal(0);
        Constant lastKey = contents.getDataVal(contents.getNumRecords() - 1);
        if (lastKey.equals(firstKey)) {
            // Create overflow block
            BlockId newBlockId = contents.split(1, contents.getFlag());
            contents.setFlag(newBlockId.blockNum());
            return null;
        } else {
            // We want to move the key with the same value to the same block
            int splitPos = contents.getNumRecords() / 2;
            Constant splitKey = contents.getDataVal(splitPos);
            if (splitKey.equals(firstKey)) {
                while (contents.getDataVal(splitPos).equals(splitKey)) {
                    splitPos++;
                }
                splitKey = contents.getDataVal(splitPos);
            } else {
                while (contents.getDataVal(splitPos - 1).equals(splitKey)) {
                    splitPos--;
                }
            }
            BlockId newBlockId = contents.split(splitPos, -1);
            return new DirectoryEntry(splitKey, newBlockId.blockNum());
        }
    }

    private boolean tryOverflow() {
        Constant firstKey = contents.getDataVal(0);
        int flag = contents.getFlag();
        if (!searchKey.equals(firstKey) || flag < 0) {
            return false;
        }
        contents.close();

        BlockId nextBlockId = new BlockId(fileName, flag);
        contents = new BTreePage(tx, nextBlockId, layout);
        currentSlot = 0;
        return true;
    }
}
