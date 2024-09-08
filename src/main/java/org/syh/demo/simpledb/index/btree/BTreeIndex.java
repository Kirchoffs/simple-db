package org.syh.demo.simpledb.index.btree;

import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.index.Index;
import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.record.FieldType;
import org.syh.demo.simpledb.record.Layout;
import org.syh.demo.simpledb.record.Rid;
import org.syh.demo.simpledb.record.Schema;
import org.syh.demo.simpledb.transaction.Transaction;

public class BTreeIndex implements Index {
    private Transaction tx;
    private Layout directoryLayout, leafLayout;
    private String leafTableName;
    private BTreeLeaf leaf;
    private BlockId rootBlockId;

    public BTreeIndex(Transaction tx, String indexName, Layout leafLayout) {
        this.tx = tx;
        this.leafTableName = indexName + ".leaf";
        this.leafLayout = leafLayout;
        if (tx.size(leafTableName) == 0) {
            BlockId blockId = tx.append(leafTableName);
            BTreePage node = new BTreePage(tx, blockId, leafLayout);
            node.format(blockId, -1);
        }

        Schema directorySchema = new Schema();
        directorySchema.add("blockNum", leafLayout.schema());
        directorySchema.add("dataVal", leafLayout.schema());
        String directoryTableName = indexName + ".dir";
        directoryLayout = new Layout(directorySchema);
        rootBlockId = new BlockId(directoryTableName, 0);
        if (tx.size(directoryTableName) == 0) {
            tx.append(directoryTableName);
            BTreePage node = new BTreePage(tx, rootBlockId, directoryLayout);
            node.format(rootBlockId, 0);
            FieldType fieldType = directorySchema.getType("dataVal");
            Constant minVal = fieldType == FieldType.INTEGER ? new Constant(Integer.MIN_VALUE) : new Constant("");
            // Insert a virtual key which is smaller than any other key, and a child block pointer with block number 0
            node.insertDirectoryData(0, minVal, 0); // Point to the first leaf block
            // Then, when we insert entry (dataVal, blockNum) in the following operations,
            // dataVal is always smaller than or equal to the first key in the directory page with blockNum
            node.close();
        }
    }

    @Override
    public void beforeFirst(Constant searchKey) {
        close();
        BTreeDirectory root = new BTreeDirectory(tx, rootBlockId, directoryLayout);
        // Find the block whose dataVal is smaller than or equal to the searchKey
        // dataVal(currentBlockId) <= searchKey < dataVal(nextBlockId)
        int blockNum = root.search(searchKey);
        root.close();
        BlockId leafBlockId = new BlockId(leafTableName, blockNum);
        leaf = new BTreeLeaf(tx, leafBlockId, leafLayout, searchKey);
    }

    @Override
    public boolean next() {
        return leaf.next();
    }

    @Override
    public Rid getDataRid() {
        return leaf.getDataRid();
    }

    @Override
    public void insert(Constant dataVal, Rid dataRid) {
        beforeFirst(dataVal); // Find the leaf page whose dataVal is smaller than or equal to the input dataVal
        // Inside leaf page object, the searchKey is dataVal
        // So in the leaf page object, its dataVal is smaller than or equal to its searchKey
        DirectoryEntry directoryEntry = leaf.insert(dataRid);
        leaf.close();
        if (directoryEntry == null) {
            return;
        }
        BTreeDirectory root = new BTreeDirectory(tx, rootBlockId, directoryLayout);
        // Insert from top down
        DirectoryEntry potentialNewEntry = root.insert(directoryEntry);
        if (potentialNewEntry != null) {
            root.makeNewRoot(potentialNewEntry);
        }
        root.close();
    }

    @Override
    public void delete(Constant dataVal, Rid dataRid) {
        beforeFirst(dataVal);
        leaf.delete(dataRid);
        leaf.close();
    }

    @Override
    public void close() {
        if (leaf != null) {
            leaf.close();
        }
    }

    @Override
    public int searchCost(int numBlocks, int rpb) {
        return 1 + (int) (Math.log(numBlocks) / Math.log(rpb));
    }
}
