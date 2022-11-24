package org.syh.demo.simpledb.jdbc.embedded;

import org.syh.demo.simpledb.jdbc.DriverAdapter;
import org.syh.demo.simpledb.server.SimpleDB;

import java.sql.SQLException;
import java.util.Properties;

public class EmbeddedDriver extends DriverAdapter {
    @Override
    public EmbeddedConnection connect(String dbName, Properties properties) throws SQLException {
        SimpleDB db = new SimpleDB(dbName);
        return new EmbeddedConnection(db);
    }
}
