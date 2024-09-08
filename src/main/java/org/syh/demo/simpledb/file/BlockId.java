package org.syh.demo.simpledb.file;

public class BlockId {
    private String fileName;
    private int blockNum;

    public BlockId(String fileName, int blockNum) {
        this.fileName = fileName;
        this.blockNum = blockNum;
    }

    public String fileName() {
        return fileName;
    }

    public int blockNum() {
        return blockNum;
    }

    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        BlockId blockId = (BlockId) obj;
        return fileName.equals(blockId.fileName) && blockNum == blockId.blockNum;
    }

    public String toString() {
        return "[file: " + fileName + ", block: " + blockNum + "]";
    }

    public int hashCode() {
        return toString().hashCode();
    }
}
