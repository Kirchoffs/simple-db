package org.syh.demo.simpledb.jdbc.network;

import org.syh.demo.simpledb.jdbc.ConnectionAdapter;
import org.syh.demo.simpledb.jdbc.network.remote.RemoteConnection;
import org.syh.demo.simpledb.jdbc.network.remote.RemoteStatement;

import java.sql.SQLException;

public class NetworkConnection extends ConnectionAdapter {
    private RemoteConnection connection;

    public NetworkConnection(RemoteConnection connection) {
        this.connection = connection;
    }

    @Override
    public NetworkStatement createStatement() throws SQLException {
        try {
            RemoteStatement remoteStatement = connection.createStatement();
            return new NetworkStatement(remoteStatement);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            connection.close();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
