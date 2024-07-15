package org.syh.demo.simpledb.jdbc.network.remote;

import java.rmi.RemoteException;

public interface RemoteStatement {
    RemoteResultSet executeQuery(String sql) throws RemoteException;
    int executeUpdate(String sql) throws RemoteException;
    void close() throws RemoteException;
}
