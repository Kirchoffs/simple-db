package org.syh.demo.simpledb.jdbc.network.remote.impl;

import org.syh.demo.simpledb.jdbc.network.remote.RemoteResultSet;
import org.syh.demo.simpledb.jdbc.network.remote.RemoteStatement;
import org.syh.demo.simpledb.plan.Plan;
import org.syh.demo.simpledb.plan.Planner;
import org.syh.demo.simpledb.transaction.Transaction;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RemoteStatementImpl extends UnicastRemoteObject implements RemoteStatement {
    private RemoteConnectionImpl connection;
    private Planner planner;

    public RemoteStatementImpl(RemoteConnectionImpl connection, Planner planner) throws RemoteException {
        this.connection = connection;
        this.planner = planner;
    }

    @Override
    public RemoteResultSet executeQuery(String sql) throws RemoteException {
        try {
            Transaction tx = connection.getTransaction();
            Plan plan = planner.createQueryPlan(sql, tx);
            return new RemoteResultSetImpl(plan, connection);
        } catch (RuntimeException e) {
            connection.rollback();
            throw e;
        }
    }

    @Override
    public int executeUpdate(String sql) throws RemoteException {
        try {
            Transaction tx = connection.getTransaction();
            int result = planner.executeMutation(sql, tx);
            connection.commit();
            return result;
        } catch (RuntimeException e) {
            connection.rollback();
            throw e;
        }
    }

    @Override
    public void close() {
    }
}
