package org.syh.demo.simpledb.jdbc.network;

import org.syh.demo.simpledb.jdbc.DriverAdapter;
import org.syh.demo.simpledb.jdbc.network.remote.RemoteConnection;
import org.syh.demo.simpledb.jdbc.network.remote.RemoteDriver;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.SQLException;
import java.util.Properties;

public class NetworkDriver extends DriverAdapter {
    public NetworkConnection connect(String url, Properties properties) throws SQLException {
        try {
            String host = url.replace("jdbc:simple-db://", "");
            Registry registry = LocateRegistry.getRegistry(host, 1024);
            RemoteDriver remoteDriver = (RemoteDriver) registry.lookup("simple-db");
            RemoteConnection remoteConnection = remoteDriver.connect();
            return new NetworkConnection(remoteConnection);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
