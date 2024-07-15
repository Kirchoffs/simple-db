package org.syh.demo.simpledb.server;

import org.syh.demo.simpledb.jdbc.network.remote.RemoteDriver;
import org.syh.demo.simpledb.jdbc.network.remote.impl.RemoteDriverImpl;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class StartServer {
    public static void main(String[] args) throws Exception {
        SimpleDBOptions options = SimpleDBOptions.defaultOptions();
        SimpleDB db = new SimpleDB(options);

        Registry registry = LocateRegistry.createRegistry(1099);

        RemoteDriver remoteDriver = new RemoteDriverImpl(db);
        registry.rebind("simple-db", remoteDriver);

        System.out.println("Database server ready...");
    }
}
