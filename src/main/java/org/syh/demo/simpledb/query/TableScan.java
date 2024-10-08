package org.syh.demo.simpledb.query;

import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.record.FieldType;
import org.syh.demo.simpledb.record.Layout;
import org.syh.demo.simpledb.record.RecordPage;
import org.syh.demo.simpledb.record.Rid;
import org.syh.demo.simpledb.transaction.Transaction;

public class TableScan implements UpdateScan {
    private Transaction tx;
    private Layout layout;
    private RecordPage recordPage;
    private String fileName;
    private int currentSlot;

    public TableScan(Transaction tx, String tableName, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        this.fileName = tableName + ".tbl";

        if (tx.size(fileName) == 0) {
            moveToNewBlock();
        } else {
            moveToBlock(0);
        }
    }

    public void close() {
        if (recordPage != null) {
            tx.unpin(recordPage.blockId());
        }
    }

    private void moveToNewBlock() {
        close();
        BlockId blockId = tx.append(fileName);
        recordPage = new RecordPage(tx, blockId, layout);
        recordPage.format();
        currentSlot = -1;
    }

    private void moveToBlock(int blockNum) {
        close();
        BlockId blockId = new BlockId(fileName, blockNum);
        recordPage = new RecordPage(tx, blockId, layout);
        currentSlot = -1;
    }

    public void beforeFirst() {
        moveToBlock(0);
    }

    // Move to the next record in the table.
    // Return false if there is no next record.
    public boolean next() {
        currentSlot = recordPage.nextAfter(currentSlot);
        while (currentSlot < 0) {
            if (atLastBlock()) {
                return false;
            }
            moveToBlock(recordPage.blockId().blockNum() + 1);
            currentSlot = recordPage.nextAfter(currentSlot);
        }
        return true;
    }

    private boolean atLastBlock() {
        return recordPage.blockId().blockNum() == tx.size(fileName) - 1;
    }

    public int getInt(String fieldName) {
        return recordPage.getInt(currentSlot, fieldName);
    }

    public String getString(String fieldName) {
        return recordPage.getString(currentSlot, fieldName);
    }

    public Constant getVal(String fieldName) {
        if (layout.schema().getType(fieldName) == FieldType.INTEGER) {
            return new Constant(getInt(fieldName));
        } else {
            return new Constant(getString(fieldName));
        }
    }

    public boolean hasField(String fieldName) {
        return layout.schema().hasField(fieldName);
    }

    public void setInt(String fieldName, int value) {
        recordPage.setInt(currentSlot, fieldName, value);
    }

    public void setString(String fieldName, String value) {
        recordPage.setString(currentSlot, fieldName, value);
    }

    public void setVal(String fieldName, Constant value) {
        if (layout.schema().getType(fieldName) == FieldType.INTEGER) {
            setInt(fieldName, value.asInt());
        } else {
            setString(fieldName, value.asString());
        }
    }

    public void insert() {
        currentSlot = recordPage.insertAfter(currentSlot);
        while (currentSlot < 0) {
            if (atLastBlock()) {
                moveToNewBlock();
            } else {
                moveToBlock(recordPage.blockId().blockNum() + 1);
            }
            currentSlot = recordPage.insertAfter(currentSlot);
        }
    }

    public void delete() {
        recordPage.delete(currentSlot);
    }

    public void moveToRid(Rid rid) {
        close();
        BlockId blockId = new BlockId(fileName, rid.blockNum());
        recordPage = new RecordPage(tx, blockId, layout);
        currentSlot = rid.slot();
    }

    public Rid getRid() {
        return new Rid(recordPage.blockId().blockNum(), currentSlot);
    }
}
