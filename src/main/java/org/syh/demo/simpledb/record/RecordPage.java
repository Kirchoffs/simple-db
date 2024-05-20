package org.syh.demo.simpledb.record;

import org.syh.demo.simpledb.file.BlockId;
import org.syh.demo.simpledb.transaction.Transaction;

public class RecordPage {
    public static final int EMPTY = 0;
    public static final int USED = 1;

    private Transaction tx;
    private BlockId blockId;
    private Layout layout;

    public RecordPage(Transaction tx, BlockId blockId, Layout layout) {
        this.tx = tx;
        this.blockId = blockId;
        this.layout = layout;
        tx.pin(blockId);
    }

    public int getInt(int slot, String fieldName) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        return tx.getInt(blockId, fieldPosition);
    }

    public String getString(int slot, String fieldName) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        return tx.getString(blockId, fieldPosition);
    }

    public void setInt(int slot, String fieldName, int value) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        tx.setInt(blockId, fieldPosition, value, true);
    }

    public void setString(int slot, String fieldName, String value) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);
        tx.setString(blockId, fieldPosition, value, true);
    }

    public void delete(int slot) {
        setFlag(slot, EMPTY);
    }

    public void format() {
        int slot = 0;
        while (isValidSlot(slot)) {
            tx.setInt(blockId, offset(slot), EMPTY, false);
            Schema schema = layout.getSchema();
            for (String field : schema.fields()) {
                int position = offset(slot) + layout.getOffset(field);
                if (schema.type(field) == java.sql.Types.INTEGER) {
                    tx.setInt(blockId, position, 0, false);
                } else {
                    tx.setString(blockId, position, "", false);
                }
            }
            slot++;
        }
    }

    public int nextAfter(int slot) {
        return searchAfter(slot, USED);
    }

    public int insertAfter(int slot) {
        int newSlot = searchAfter(slot, EMPTY);
        if (newSlot >= 0) {
            setFlag(newSlot, USED);
        }
        return newSlot;
    }

    public int searchAfter(int slot, int flag) {
        slot++;
        while (isValidSlot(slot)) {
            if (tx.getInt(blockId, offset(slot)) == flag) {
                return slot;
            }
            slot++;
        }
        return -1;
    }

    private int offset(int slot) {
        return slot * layout.getSlotSize();
    }

    private void setFlag(int slot, int flag) {
        int position = offset(slot);
        tx.setInt(blockId, position, flag, true);
    }

    public boolean isEmpty(int slot) {
        return tx.getInt(blockId, offset(slot)) == EMPTY;
    }

    public boolean isValidSlot(int slot) {
        return slot >= 0 && offset(slot + 1) <= tx.getBlockSize();
    }

    public BlockId getBlockId() {
        return blockId;
    }
}
