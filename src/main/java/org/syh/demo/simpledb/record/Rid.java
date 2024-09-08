package org.syh.demo.simpledb.record;

/**
 * RID: Record Identifier
 */
public class Rid {
    private int blockNum;
    private int slot;

    public Rid(int blockNum, int slot) {
        this.blockNum = blockNum;
        this.slot = slot;
    }

    public int blockNum() {
        return blockNum;
    }

    public int slot() {
        return slot;
    }

    public boolean equals(Object obj) {
        Rid rid = (Rid) obj;
        return blockNum == rid.blockNum && slot == rid.slot;
    }

    public String toString() {
        return "[" + blockNum + ", " + slot + "]";
    }
}
