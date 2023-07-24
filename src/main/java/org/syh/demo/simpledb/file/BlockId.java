package org.syh.demo.simpledb.file;

public class BlockId {
    private String fileName;
    private int blockNum;

    public BlockId(String fileName, int blockNum) {
        this.fileName = fileName;
        this.blockNum = blockNum;
    }

    public String getFileName() {
        return fileName;
    }

    public int getBlockNum() {
        return blockNum;
    }

    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        BlockId blk = (BlockId) obj;
        return fileName.equals(blk.fileName) && blockNum == blk.blockNum;
    }

    public String toString() {
        return "[file: " + fileName + ", block: " + blockNum + "]";
    }

    public int hashCode() {
        return toString().hashCode();
    }
}
