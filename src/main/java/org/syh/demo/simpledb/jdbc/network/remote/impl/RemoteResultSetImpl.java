package org.syh.demo.simpledb.jdbc.network.remote.impl;

import org.syh.demo.simpledb.jdbc.network.remote.RemoteResultSet;
import org.syh.demo.simpledb.jdbc.network.remote.RemoteResultSetMetaData;
import org.syh.demo.simpledb.plan.Plan;
import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.record.Schema;

import java.rmi.RemoteException;

public class RemoteResultSetImpl implements RemoteResultSet {
    private Scan scan;
    private Schema schema;
    private RemoteConnectionImpl connection;

    public RemoteResultSetImpl(Plan plan, RemoteConnectionImpl connection) {
        scan = plan.open();
        schema = plan.schema();
        this.connection = connection;
    }

    @Override
    public boolean next() throws RemoteException {
        try {
            return scan.next();
        } catch (RuntimeException e) {
            connection.rollback();
            throw e;
        }
    }

    @Override
    public int getInt(String fieldName) throws RemoteException {
        try {
            fieldName = fieldName.toLowerCase();
            return scan.getInt(fieldName);
        } catch (RuntimeException e) {
            connection.rollback();
            throw e;
        }
    }

    @Override
    public String getString(String fieldName) throws RemoteException {
        try {
            fieldName = fieldName.toLowerCase();
            return scan.getString(fieldName);
        } catch (RuntimeException e) {
            connection.rollback();
            throw e;
        }
    }

    @Override
    public RemoteResultSetMetaData getRemoteResultSetMetaData() throws RemoteException {
        return new RemoteResultSetMetaDataImpl(schema);
    }

    @Override
    public void close() throws RemoteException {
        scan.close();
        connection.commit();
    }
}
