package org.syh.demo.simpledb.index.btree;

import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.record.FieldType;
import org.syh.demo.simpledb.record.Layout;
import org.syh.demo.simpledb.record.Rid;
import org.syh.demo.simpledb.record.Schema;
import org.syh.demo.simpledb.transaction.Transaction;

public class BTreePage {
    private Transaction tx;
    private BlockId currentBlockId;
    private Layout layout;

    public BTreePage(Transaction tx, BlockId currentBlockId, Layout layout) {
        this.tx = tx;
        this.currentBlockId = currentBlockId;
        this.layout = layout;
        tx.pin(currentBlockId);
    }

    // Find the last slot whose value is less than searchKey
    public int findSlotBefore(Constant searchKey) {
        int slot = 0;
        while (slot < getNumRecords() && getDataVal(slot).compareTo(searchKey) < 0) {
            slot++;
        }
        return slot - 1;
    }

    public int getNumRecords() {
        return tx.getInt(currentBlockId, Integer.BYTES);
    }

    private void setNumRecords(int numRecords) {
        tx.setInt(currentBlockId, Integer.BYTES, numRecords, true);
    }

    private Constant getVal(int slot, String fieldName) {
        FieldType type = layout.schema().getType(fieldName);
        if (type == FieldType.INTEGER) {
            return new Constant(getInt(slot, fieldName));
        } else {
            return new Constant(getString(slot, fieldName));
        }
    }

    public void setVal(int slot, String fieldName, Constant val) {
        FieldType fieldType = layout.schema().getType(fieldName);
        if (fieldType == FieldType.INTEGER) {
            setInt(slot, fieldName, val.asInt());
        } else {
            setString(slot, fieldName, val.asString());
        }
    }

    // A directory page uses flag to hold its level
    // A leaf page uses flag to point to its overflow block
    public int getFlag() {
        return tx.getFirstInt(currentBlockId);
    }

    public void setFlag(int val) {
        tx.setFirstInt(currentBlockId, val, true);
    }

    private int getInt(int slot, String fieldName) {
        int pos = getFieldPos(slot, fieldName);
        return tx.getInt(currentBlockId, pos);
    }

    private void setInt(int slot, String fieldName, int val) {
        int pos = getFieldPos(slot, fieldName);
        tx.setInt(currentBlockId, pos, val, true);
    }

    private String getString(int slot, String fieldName) {
        int pos = getFieldPos(slot, fieldName);
        return tx.getString(currentBlockId, pos);
    }

    private void setString(int slot, String fieldName, String val) {
        int pos = getFieldPos(slot, fieldName);
        tx.setString(currentBlockId, pos, val, true);
    }

    public int getBlockNum(int slot) {
        return getInt(slot, "blockNum");
    }

    public Constant getDataVal(int slot) {
        return getVal(slot, "dataVal");
    }

    public boolean isFull() {
        return getSlotPos(getNumRecords() + 1) >= tx.getBlockSize();
    }

    // Return the new block id containing the records from splitPos to the end
    public BlockId split(int splitPos, int flag) {
        BlockId newBlockId = appendNew(flag);
        BTreePage newPage = new BTreePage(tx, newBlockId, layout);
        transferRecords(splitPos, newPage);
        newPage.setFlag(flag);
        newPage.close();
        return newBlockId;
    }

    private void transferRecords(int srcPos, BTreePage dstPage) {
        int dstPos = 0;
        while (srcPos < getNumRecords()) {
            dstPage.insert(dstPos);
            Schema schema = layout.schema();
            for (String fieldName : schema.fields()) {
                dstPage.setVal(dstPos, fieldName, getVal(srcPos, fieldName));
            }
            delete(srcPos);
            dstPos++;
        }
    }

    public void insert(int slot) {
        for (int i = getNumRecords(); i > slot; i--) {
            copyRecord(i - 1, i);
        }
        setNumRecords(getNumRecords() + 1);
    }

    public void delete(int slot) {
        for (int i = slot + 1; i < getNumRecords(); i++) {
            copyRecord(i, i - 1);
        }
        setNumRecords(getNumRecords() - 1);
    }

    private void copyRecord(int from, int to) {
        Schema schema = layout.schema();
        for (String fieldName : schema.fields()) {
            setVal(to, fieldName, getVal(from, fieldName));
        }
    }

    public void close() {
        if (currentBlockId != null) {
            tx.unpin(currentBlockId);
        }
        currentBlockId = null;
    }

    // dataVal of "blockNum" is greater than or equal to "val"
    public void insertDirectoryData(int slot, Constant val, int blockNum) {
        insert(slot);
        setVal(slot, "dataVal", val);
        setInt(slot, "blockNum", blockNum);
    }

    public void insertLeafData(int slot, Constant searchKey, Rid dataRid) {
        insert(slot);
        setVal(slot, "dataVal", searchKey);
        setInt(slot, "blockNum", dataRid.blockNum());
        setInt(slot, "slot", dataRid.slot());
    }

    public Rid getDataRid(int slot) {
        return new Rid(getInt(slot, "blockNum"), getInt(slot, "slot"));
    }

    private BlockId appendNew(int flag) {
        BlockId newBlockId = tx.append(currentBlockId.fileName());
        tx.pin(newBlockId);
        format(newBlockId, flag);
        return newBlockId;
    }

    public void format(BlockId blockId, int flag) {
        tx.setInt(blockId, 0, flag, false);        // Flag
        tx.setInt(blockId, Integer.BYTES, 0, false); // Number of records
        int recordSize = layout.slotSize();
        for (int pos = 2 * Integer.BYTES; pos + recordSize <= tx.getBlockSize(); pos += recordSize) {
            makeDefaultRecord(blockId, pos);
        }
    }

    private void makeDefaultRecord(BlockId blockId, int pos) {
        for (String fieldName : layout.schema().fields()) {
            FieldType type = layout.schema().getType(fieldName);
            int offset = layout.getOffset(fieldName);
            if (type == FieldType.INTEGER) {
                tx.setInt(blockId, pos + offset, 0, false);
            } else {
                tx.setString(blockId, pos + offset, "", false);
            }
        }
    }

    private int getFieldPos(int slot, String fieldName) {
        return layout.getOffset(fieldName) + getSlotPos(slot);
    }

    private int getSlotPos(int slot) {
        int slotSize = layout.slotSize();
        // The first two integers are flag and number of records
        return Integer.BYTES + Integer.BYTES + slot * slotSize;
    }
}
