package org.syh.demo.simpledb.record;

import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.query.Constant;
import org.syh.demo.simpledb.transaction.Transaction;

public class TableScan {
    private Transaction tx;
    private Layout layout;
    private RecordPage recordPage;
    private String fileName;
    private int currentSlot;

    public TableScan(Transaction tx, String tableName, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        this.fileName = tableName;

        if (tx.size(fileName) == 0) {
            moveToNewBlock();
        } else {
            moveToBlock(0);
        }
    }

    public void close() {
        if (recordPage != null) {
            tx.unpin(recordPage.getBlockId());
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

    public boolean next() {
        currentSlot = recordPage.nextAfter(currentSlot);
        while (currentSlot < 0) {
            if (atLastBlock()) {
                return false;
            }
            moveToBlock(recordPage.getBlockId().getBlockNum() + 1);
            currentSlot = recordPage.nextAfter(currentSlot);
        }
        return true;
    }

    private boolean atLastBlock() {
        return recordPage.getBlockId().getBlockNum() == tx.size(fileName) - 1;
    }

    public int getInt(String fieldName) {
        return recordPage.getInt(currentSlot, fieldName);
    }

    public String getString(String fieldName) {
        return recordPage.getString(currentSlot, fieldName);
    }

    public Constant getVal(String fieldName) {
        if (layout.getSchema().type(fieldName) == java.sql.Types.INTEGER) {
            return new Constant(getInt(fieldName));
        } else {
            return new Constant(getString(fieldName));
        }
    }

    public boolean hasField(String fieldName) {
        return layout.getSchema().hasField(fieldName);
    }

    public void setInt(String fieldName, int value) {
        recordPage.setInt(currentSlot, fieldName, value);
    }

    public void setString(String fieldName, String value) {
        recordPage.setString(currentSlot, fieldName, value);
    }

    public void setVal(String fieldName, Constant value) {
        if (layout.getSchema().type(fieldName) == java.sql.Types.INTEGER) {
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
                moveToBlock(recordPage.getBlockId().getBlockNum() + 1);
            }
            currentSlot = recordPage.insertAfter(currentSlot);
        }
    }

    public void delete() {
        recordPage.delete(currentSlot);
    }

    public void moveToRid(RID rid) {
        close();
        BlockId blockId = new BlockId(fileName, rid.getBlockNum());
        recordPage = new RecordPage(tx, blockId, layout);
        currentSlot = rid.getSlot();
    }

    public RID getRid() {
        return new RID(recordPage.getBlockId().getBlockNum(), currentSlot);
    }
}
