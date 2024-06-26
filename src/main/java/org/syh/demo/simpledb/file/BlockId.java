package org.syh.demo.simpledb.file;

public class BlockId {
    private String filename;
    private int blockNum;

    public BlockId(String filename, int blockNum) {
        this.filename = filename;
        this.blockNum = blockNum;
    }

    public String getFilename() {
        return filename;
    }

    public int getBlockNum() {
        return blockNum;
    }

    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        BlockId blockId = (BlockId) obj;
        return filename.equals(blockId.filename) && blockNum == blockId.blockNum;
    }

    public String toString() {
        return "[file: " + filename + ", block: " + blockNum + "]";
    }

    public int hashCode() {
        return toString().hashCode();
    }
}
