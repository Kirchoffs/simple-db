package org.syh.demo.simpledb.jdbc.network;

import org.syh.demo.simpledb.jdbc.ResultSetMetaDataAdapter;
import org.syh.demo.simpledb.jdbc.network.remote.RemoteResultSetMetaData;

import java.sql.SQLException;

public class NetworkResultSetMetaData extends ResultSetMetaDataAdapter {
    private RemoteResultSetMetaData resultSetMetaData;

    public NetworkResultSetMetaData(RemoteResultSetMetaData resultSetMetaData) {
        this.resultSetMetaData = resultSetMetaData;
    }

    public int getColumnCount() throws SQLException {
        try {
            return resultSetMetaData.getColumnCount();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public String getColumnName(int column) throws SQLException {
        try {
            return resultSetMetaData.getColumnName(column);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public int getColumnType(int column) throws SQLException {
        try {
            return resultSetMetaData.getColumnType(column);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public int getColumnDisplaySize(int column) throws SQLException {
        try {
            return resultSetMetaData.getColumnDisplaySize(column);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
