package org.syh.demo.simpledb.plan;

import org.syh.demo.simpledb.parse.data.CreateIndexData;
import org.syh.demo.simpledb.parse.data.CreateTableData;
import org.syh.demo.simpledb.parse.data.CreateViewData;
import org.syh.demo.simpledb.parse.data.DeleteData;
import org.syh.demo.simpledb.parse.data.InsertData;
import org.syh.demo.simpledb.parse.Parser;
import org.syh.demo.simpledb.parse.data.QueryData;
import org.syh.demo.simpledb.parse.data.UpdateData;
import org.syh.demo.simpledb.plan.planner.MutationPlanner;
import org.syh.demo.simpledb.plan.planner.QueryPlanner;
import org.syh.demo.simpledb.transaction.Transaction;

public class Planner {
    private QueryPlanner queryPlanner;
    private MutationPlanner mutationPlanner;

    public Planner(QueryPlanner queryPlanner, MutationPlanner mutationPlanner) {
        this.queryPlanner = queryPlanner;
        this.mutationPlanner = mutationPlanner;
    }

    public Plan createQueryPlan(String query, Transaction tx) {
        Parser parser = new Parser(query);
        QueryData data = parser.query();
        verifyQuery(data);
        return queryPlanner.createPlan(data, tx);
    }

    public int executeMutation(String mutation, Transaction tx) {
        Parser parser = new Parser(mutation);
        Object data = parser.mutation();
        verifyMutation(data);

        if (data instanceof InsertData) {
            return mutationPlanner.executeInsert((InsertData) data, tx);
        } else if (data instanceof DeleteData) {
            return mutationPlanner.executeDelete((DeleteData) data, tx);
        } else if (data instanceof UpdateData) {
            return mutationPlanner.executeUpdate((UpdateData) data, tx);
        } else if (data instanceof CreateTableData) {
            return mutationPlanner.executeCreateTable((CreateTableData) data, tx);
        } else if (data instanceof CreateViewData) {
            return mutationPlanner.executeCreateView((CreateViewData) data, tx);
        } else if (data instanceof CreateIndexData) {
            return mutationPlanner.executeCreateIndex((CreateIndexData) data, tx);
        } else {
            return 0;
        }
    }

    private void verifyQuery(QueryData data) {
        // TODO: verify query
    }

    private void verifyMutation(Object data) {
        // TODO: verify mutation
    }
}
