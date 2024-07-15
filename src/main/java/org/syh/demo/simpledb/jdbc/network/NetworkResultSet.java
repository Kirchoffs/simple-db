package org.syh.demo.simpledb.jdbc.network;

import org.syh.demo.simpledb.jdbc.ResultSetAdapter;
import org.syh.demo.simpledb.jdbc.network.remote.RemoteResultSet;

import java.sql.SQLException;

public class NetworkResultSet extends ResultSetAdapter {
    private RemoteResultSet resultSet;

    public NetworkResultSet(RemoteResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public boolean next() throws SQLException {
        try {
            return resultSet.next();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public int getInt(String fieldName) throws SQLException {
        try {
            return resultSet.getInt(fieldName);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public String getString(String fieldName) throws SQLException {
        try {
            return resultSet.getString(fieldName);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public NetworkResultSetMetaData getMetaData() throws SQLException {
        try {
            return new NetworkResultSetMetaData(resultSet.getRemoteResultSetMetaData());
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public void close() throws SQLException {
        try {
            resultSet.close();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
