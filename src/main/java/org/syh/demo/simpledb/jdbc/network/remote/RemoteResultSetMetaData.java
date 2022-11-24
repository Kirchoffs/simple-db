package org.syh.demo.simpledb.jdbc.network.remote;

import java.rmi.RemoteException;

public interface RemoteResultSetMetaData {
    int getColumnCount() throws RemoteException;
    String getColumnName(int column) throws RemoteException;
    int getColumnType(int column) throws RemoteException;
    int getColumnDisplaySize(int column) throws RemoteException;
}
