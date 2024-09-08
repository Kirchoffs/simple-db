package org.syh.demo.simpledb.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

public class FileManager {
    private File dbDirectory;
    private int blockSize;
    private boolean isNew;
    private BigInteger numBlocksRead;
    private BigInteger numBlocksWritten;
    private Map<String, RandomAccessFile> openFiles = new HashMap<>();

    public FileManager(File dbDirectory, int blockSize) {
        this.dbDirectory = dbDirectory;
        this.blockSize = blockSize;
        isNew = checkIsNew(dbDirectory);

        numBlocksRead = BigInteger.ZERO;
        numBlocksWritten = BigInteger.ZERO;

        if (isNew) {
            dbDirectory.mkdirs();
        }

        for (String fileName : dbDirectory.list()) {
            if (fileName.startsWith("temp")) {
                new File(dbDirectory, fileName).delete();
            }
        }
    }

    private boolean checkIsNew(File dbDirectory) {
        String[] files = dbDirectory.list();
        if (!dbDirectory.exists() || files == null) {
            return true;
        }
        return files.length == 0 || (files.length == 1 && ".gitkeep".equals(files[0]));
    }

    /**
     * Read from disk to page
     * @param blockId
     * @param page
     */
    public synchronized void read(BlockId blockId, Page page) {
        try {
            RandomAccessFile f = getFile(blockId.fileName());
            f.seek(blockId.blockNum() * blockSize);
            f.getChannel().read(page.contents());
            numBlocksRead = numBlocksRead.add(BigInteger.ONE);
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + blockId);
        }
    }

    /**
     * Write to disk from page
     * @param blockId
     * @param page
     */
    public synchronized void write(BlockId blockId, Page page) {
        try {
            RandomAccessFile f = getFile(blockId.fileName());
            f.seek(blockId.blockNum() * blockSize);
            f.getChannel().write(page.contents());
            numBlocksWritten = numBlocksWritten.add(BigInteger.ONE);
        } catch (IOException e) {
            throw new RuntimeException("cannot write block " + blockId);
        }
    }

    public synchronized BlockId append(String fileName) {
        int newBlockNum = length(fileName);
        BlockId blockId = new BlockId(fileName, newBlockNum);
        byte[] bytes = new byte[blockSize];
        try {
            RandomAccessFile f = getFile(fileName);
            f.seek(newBlockNum * blockSize);
            f.write(bytes);
        } catch (IOException e) {
            throw new RuntimeException("cannot append block " + blockId);
        }
        return blockId;
    }

    public int length(String fileName) {
        try {
            RandomAccessFile f = getFile(fileName);
            return (int) (f.length() / blockSize);
        }
        catch (IOException e) {
            throw new RuntimeException("cannot access " + fileName);
        }
    }

    private RandomAccessFile getFile(String fileName) throws IOException {
        RandomAccessFile f = openFiles.get(fileName);
        if (f == null) {
            File dbFile = new File(dbDirectory, fileName);
            // The “s” portion specifies that the operating system should not delay disk I/O in order to optimize disk performance.
            // Instead, every write operation must be written immediately to the disk.
            f = new RandomAccessFile(dbFile, "rws");
            openFiles.put(fileName, f);
        }
        return f;
    }

    public int blockSize() {
        return blockSize;
    }

    public boolean isNew() {
        return isNew;
    }

    public void getStatistics() {
        System.out.println(String.format("Number of blocks read: %s so far", numBlocksRead));
        System.out.println(String.format("Number of blocks written: %s so far", numBlocksWritten));
    }
}
