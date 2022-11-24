package org.syh.demo.simpledb.jdbc.embedded;

import org.syh.demo.simpledb.jdbc.StatementAdapter;
import org.syh.demo.simpledb.plan.Plan;
import org.syh.demo.simpledb.plan.Planner;
import org.syh.demo.simpledb.transaction.Transaction;

import java.sql.SQLException;

public class EmbeddedStatement extends StatementAdapter {
    private EmbeddedConnection connection;
    private Planner planner;

    public EmbeddedStatement(EmbeddedConnection connection, Planner planner) {
        this.connection = connection;
        this.planner = planner;
    }

    public EmbeddedResultSet executeQuery(String sql) throws SQLException {
        try {
            Transaction tx = connection.getTransaction();
            Plan plan = planner.createQueryPlan(sql, tx);
            return new EmbeddedResultSet(plan, connection);
        } catch (RuntimeException e) {
            connection.rollback();
            throw new SQLException(e);
        }
    }

    public int executeUpdate(String sql) throws SQLException {
        try {
            Transaction tx = connection.getTransaction();
            int result = planner.executeMutation(sql, tx);
            connection.commit();
            return result;
        } catch (RuntimeException e) {
            connection.rollback();
            throw new SQLException(e);
        }
    }

    public void close() {
    }
}
