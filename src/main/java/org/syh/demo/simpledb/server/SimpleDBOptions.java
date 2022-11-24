package org.syh.demo.simpledb.server;

public class SimpleDBOptions {
    private String dirName;
    private int blockSize;
    private int bufferSize;

    private SimpleDBOptions(Builder builder) {
        this.dirName = builder.dirName;
        this.blockSize = builder.blockSize;
        this.bufferSize = builder.bufferSize;
    }

    public String getDirName() {
        return dirName;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public static SimpleDBOptions defaultOptions() {
        return new Builder()
                .dirName("simple-db")
                .blockSize(1024)
                .bufferSize(8)
                .build();
    }

    public static class Builder {
        private String dirName;
        private int blockSize;
        private int bufferSize;

        public Builder dirName(String dirName) {
            this.dirName = dirName;
            return this;
        }

        public Builder blockSize(int blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        public Builder bufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        public SimpleDBOptions build() {
            return new SimpleDBOptions(this);
        }
    }
}
