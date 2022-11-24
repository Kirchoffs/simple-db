package org.syh.demo.simpledb.jdbc.network.remote.impl;

import org.syh.demo.simpledb.jdbc.network.remote.RemoteConnection;
import org.syh.demo.simpledb.jdbc.network.remote.RemoteDriver;
import org.syh.demo.simpledb.server.SimpleDB;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RemoteDriverImpl extends UnicastRemoteObject implements RemoteDriver {
    private SimpleDB db;

    public RemoteDriverImpl(SimpleDB db) throws RemoteException {
        this.db = db;
    }

    public RemoteConnection connect() throws RemoteException {
        return new RemoteConnectionImpl(db);
    }
}
