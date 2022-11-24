package org.syh.demo.simpledb.server;

import org.syh.demo.simpledb.buffer.BufferManager;
import org.syh.demo.simpledb.file.FileManager;
import org.syh.demo.simpledb.log.LogManager;
import org.syh.demo.simpledb.metadata.MetadataManager;
import org.syh.demo.simpledb.plan.Planner;
import org.syh.demo.simpledb.plan.planner.BasicMutationPlanner;
import org.syh.demo.simpledb.plan.planner.BasicQueryPlanner;
import org.syh.demo.simpledb.plan.planner.MutationPlanner;
import org.syh.demo.simpledb.plan.planner.QueryPlanner;
import org.syh.demo.simpledb.transaction.Transaction;

import java.io.File;

public class SimpleDB {
    public static int BLOCK_SIZE = 1024;
    public static int BUFFER_SIZE = 8;
    public static String LOG_FILE = "simple-db.log";

    private FileManager fileManager;
    private LogManager logManager;
    private BufferManager bufferManager;
    private MetadataManager metadataManager;
    private Planner planner;

    public SimpleDB(SimpleDBOptions options) {
        this(options.getDirName(), options.getBlockSize(), options.getBufferSize());
    }

    public SimpleDB(String dirName, int blockSize, int bufferSize) {
        File dbDirectory = new File(dirName);

        fileManager = new FileManager(dbDirectory, blockSize);
        logManager = new LogManager(fileManager, LOG_FILE);
        bufferManager = new BufferManager(fileManager, logManager, bufferSize);
    }

    public SimpleDB(String dirName) {
        this(dirName, BLOCK_SIZE, BUFFER_SIZE);
        Transaction tx = newTx();

        boolean isNew = fileManager.isNew();
        if (isNew) {
            System.out.println("Creating new database");
        } else {
            System.out.println("Recovering existing database");
            tx.recover();
        }

        metadataManager = new MetadataManager(isNew, tx);
        QueryPlanner queryPlanner = new BasicQueryPlanner(metadataManager);
        MutationPlanner mutationPlanner = new BasicMutationPlanner(metadataManager);
        planner = new Planner(queryPlanner, mutationPlanner);

        tx.commit();
    }

    public Transaction newTx() {
        return new Transaction(fileManager, logManager, bufferManager);
    }

    public FileManager fileManager() {
        return fileManager;
    }

    public LogManager logManager() {
        return logManager;
    }

    public BufferManager bufferManager() {
        return bufferManager;
    }

    public MetadataManager metadataManager() {
        return metadataManager;
    }

    public Planner planner() {
        return planner;
    }
}
