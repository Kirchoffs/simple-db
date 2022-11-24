package org.syh.demo.simpledb.jdbc.embedded;

import org.syh.demo.simpledb.jdbc.ResultSetAdapter;
import org.syh.demo.simpledb.plan.Plan;
import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.record.Schema;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class EmbeddedResultSet extends ResultSetAdapter {
    private Scan scan;
    private Schema schema;
    private EmbeddedConnection connection;

    public EmbeddedResultSet(Plan plan, EmbeddedConnection connection) {
        scan = plan.open();
        schema = plan.schema();
        this.connection = connection;
    }

    @Override
    public boolean next() throws SQLException {
        try {
            return scan.next();
        } catch (RuntimeException e) {
            connection.rollback();
            throw new SQLException(e);
        }
    }

    @Override
    public int getInt(String fieldName) throws SQLException {
        try {
            fieldName = fieldName.toLowerCase();
            return scan.getInt(fieldName);
        } catch (RuntimeException e) {
            connection.rollback();
            throw new SQLException(e);
        }
    }

    @Override
    public String getString(String fieldName) throws SQLException {
        try {
            fieldName = fieldName.toLowerCase();
            return scan.getString(fieldName);
        } catch (RuntimeException e) {
            connection.rollback();
            throw new SQLException(e);
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new EmbeddedResultSetMetaData(schema);
    }

    @Override
    public void close() {
        scan.close();
        connection.commit();
    }
}
