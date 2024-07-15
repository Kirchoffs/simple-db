package org.syh.demo.simpledb.jdbc.network.remote;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteDriver extends Remote {
    RemoteConnection connect() throws RemoteException;
}
