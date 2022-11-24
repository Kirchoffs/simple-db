package org.syh.demo.simpledb.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

public abstract class DriverAdapter implements Driver {
    public boolean acceptsURL(String url) {
        throw new UnsupportedOperationException("operation not implemented");
    }

    public Connection connect(String url, Properties info) throws SQLException {
        throw new UnsupportedOperationException("operation not implemented");
    }

    public int getMajorVersion() {
        return 0;
    }

    public int getMinorVersion() {
        return 0;
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return null;
    }

    public boolean jdbcCompliant() {
        return false;
    }

    public Logger getParentLogger() {
        throw new UnsupportedOperationException("operation not implemented");
    }
}
