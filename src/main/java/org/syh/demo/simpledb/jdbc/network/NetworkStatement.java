package org.syh.demo.simpledb.jdbc.network;

import org.syh.demo.simpledb.jdbc.StatementAdapter;
import org.syh.demo.simpledb.jdbc.network.remote.RemoteResultSet;
import org.syh.demo.simpledb.jdbc.network.remote.RemoteStatement;

import java.sql.SQLException;

public class NetworkStatement extends StatementAdapter {
    private RemoteStatement statement;

    public NetworkStatement(RemoteStatement statement) {
        this.statement = statement;
    }

    @Override
    public NetworkResultSet executeQuery(String sql) throws SQLException {
        try {
            RemoteResultSet remoteResultSet = statement.executeQuery(sql);
            return new NetworkResultSet(remoteResultSet);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        try {
            return statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            statement.close();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
