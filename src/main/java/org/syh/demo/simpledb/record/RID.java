package org.syh.demo.simpledb.record;

/**
 * RID: Record Identifier
 */
public class RID {
    private int blockNum;
    private int slot;

    public RID(int blockNum, int slot) {
        this.blockNum = blockNum;
        this.slot = slot;
    }

    public int getBlockNum() {
        return blockNum;
    }

    public int getSlot() {
        return slot;
    }

    public boolean equals(Object obj) {
        RID rid = (RID) obj;
        return blockNum == rid.blockNum && slot == rid.slot;
    }

    public String toString() {
        return "[" + blockNum + ", " + slot + "]";
    }
}
