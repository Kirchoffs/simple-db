package org.syh.demo.simpledb.jdbc.embedded;

import org.syh.demo.simpledb.jdbc.ConnectionAdapter;
import org.syh.demo.simpledb.plan.Planner;
import org.syh.demo.simpledb.server.SimpleDB;
import org.syh.demo.simpledb.transaction.Transaction;

public class EmbeddedConnection extends ConnectionAdapter {
    private SimpleDB db;
    private Transaction transaction;
    private Planner planner;

    public EmbeddedConnection(SimpleDB db) {
        this.db = db;
        transaction = db.newTx();
        planner = db.planner();
    }

    @Override
    public EmbeddedStatement createStatement() {
        return new EmbeddedStatement(this, planner);
    }

    @Override
    public void close() {
        transaction.commit();
    }

    @Override
    public void commit() {
        transaction.commit();
        transaction = db.newTx();
    }

    @Override
    public void rollback() {
        transaction.rollback();
        transaction = db.newTx();
    }

    public Transaction getTransaction() {
        return transaction;
    }
}
