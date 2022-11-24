package org.syh.demo.simpledb.jdbc.network.remote;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteConnection extends Remote {
    RemoteStatement createStatement() throws RemoteException;
    void close() throws RemoteException;
}
