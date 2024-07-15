package org.syh.demo.simpledb.jdbc.network.remote.impl;

import org.syh.demo.simpledb.jdbc.network.remote.RemoteConnection;
import org.syh.demo.simpledb.jdbc.network.remote.RemoteStatement;
import org.syh.demo.simpledb.plan.Planner;
import org.syh.demo.simpledb.server.SimpleDB;
import org.syh.demo.simpledb.transaction.Transaction;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RemoteConnectionImpl extends UnicastRemoteObject implements RemoteConnection {
    private SimpleDB db;
    private Transaction currentTx;
    private Planner planner;

    public RemoteConnectionImpl(SimpleDB db) throws RemoteException {
        this.db = db;
        currentTx = db.newTx();
        planner = db.planner();
    }

    @Override
    public RemoteStatement createStatement() throws RemoteException {
        return new RemoteStatementImpl(this, planner);
    }

    @Override
    public void close() throws RemoteException {
        currentTx.commit();
    }

    public Transaction getTransaction() {
        return currentTx;
    }

    public void commit() throws RemoteException {
        currentTx.commit();
        currentTx = db.newTx();
    }

    public void rollback() throws RemoteException {
        currentTx.rollback();
        currentTx = db.newTx();
    }
}
