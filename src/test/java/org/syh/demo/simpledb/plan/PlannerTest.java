package org.syh.demo.simpledb.plan;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.syh.demo.simpledb.buffer.BufferManager;
import org.syh.demo.simpledb.file.FileManager;
import org.syh.demo.simpledb.log.LogManager;
import org.syh.demo.simpledb.metadata.MetadataManager;
import org.syh.demo.simpledb.plan.planner.BasicMutationPlanner;
import org.syh.demo.simpledb.plan.planner.BasicQueryPlanner;
import org.syh.demo.simpledb.plan.planner.MutationPlanner;
import org.syh.demo.simpledb.plan.planner.QueryPlanner;
import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.transaction.Transaction;

import java.io.File;

public class PlannerTest {
    private static final String DIR_PATH = "src/test/resources/plan-test/";
    private static final String LOG_FILE_NAME = "test-log-file";
    private static final int BLOCK_SIZE = 8192;

    private File directory;

    @Before
    public void setup() {
        clearDirectory();
    }

    @After
    public void cleanup() {
        clearDirectory();
    }

    private void clearDirectory() {
        directory = new File(DIR_PATH);
        for (File file : directory.listFiles()) {
            if (!file.getName().equals(".gitkeep")) {
                file.delete();
            }
        }
    }

    @Test
    public void testPlanner() {
        FileManager fileManager = new FileManager(directory, BLOCK_SIZE);
        LogManager logManager = new LogManager(fileManager, LOG_FILE_NAME);
        BufferManager bufferManager = new BufferManager(fileManager, logManager, 3);

        Transaction tx = new Transaction(fileManager, logManager, bufferManager);

        boolean isNew = fileManager.isNew();
        MetadataManager metadataManager = new MetadataManager(isNew, tx);
        QueryPlanner queryPlanner = new BasicQueryPlanner(metadataManager);
        MutationPlanner updatePlanner = new BasicMutationPlanner(metadataManager);
        Planner planner = new Planner(queryPlanner, updatePlanner);

        String createTableSql = "CREATE TABLE test_table (x INT, y VARCHAR(16))";
        planner.executeMutation(createTableSql, tx);

        int n = 256;
        for (int i = 0; i < n; i++) {
            String insertSql = String.format("INSERT INTO test_table (x, y) VALUES (%d, 'test-%d')", i, i);
            planner.executeMutation(insertSql, tx);
        }

        String selectSql = "SELECT x, y FROM test_table WHERE x = 128";
        Plan plan = planner.createQueryPlan(selectSql, tx);
        plan.open();
        Scan scan = plan.open();
        while (scan.next()) {
            System.out.println(scan.getVal("x") + " " + scan.getVal("y"));
            Assert.assertEquals(128, scan.getInt("x"));
            Assert.assertEquals("test-128", scan.getString("y"));
        }
        scan.close();
        tx.commit();
    }
}
