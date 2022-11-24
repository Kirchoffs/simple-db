package org.syh.demo.simpledb.plan.planner;

import org.syh.demo.simpledb.parse.data.QueryData;
import org.syh.demo.simpledb.plan.Plan;
import org.syh.demo.simpledb.transaction.Transaction;

public interface QueryPlanner {
    Plan createPlan(QueryData data, Transaction tx);
}
