package org.syh.demo.simpledb.plan.planner;

import org.syh.demo.simpledb.metadata.MetadataManager;
import org.syh.demo.simpledb.parse.Parser;
import org.syh.demo.simpledb.parse.data.QueryData;
import org.syh.demo.simpledb.plan.Plan;
import org.syh.demo.simpledb.plan.ProductPlan;
import org.syh.demo.simpledb.plan.ProjectPlan;
import org.syh.demo.simpledb.plan.SelectPlan;
import org.syh.demo.simpledb.plan.TablePlan;
import org.syh.demo.simpledb.transaction.Transaction;

import java.util.ArrayDeque;
import java.util.Queue;

public class ImprovedBasicQueryPlanner implements QueryPlanner {
    private MetadataManager metadataManager;

    public ImprovedBasicQueryPlanner(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    public Plan createPlan(QueryData data, Transaction tx) {
        Queue<Plan> plans = new ArrayDeque<>();
        for (String tableName : data.tableNames()) {
            String viewDef = metadataManager.getViewDef(tableName, tx);
            if (viewDef != null) {
                Parser parser = new Parser(viewDef);
                QueryData viewData = parser.query();
                plans.offer(createPlan(viewData, tx));
            } else {
                plans.offer(new TablePlan(tx, tableName, metadataManager));
            }
        }

        Plan plan = plans.poll();
        for (Plan nextPlan : plans) {
            Plan firstPlan = new ProductPlan(nextPlan, plan);
            Plan secondPlan = new ProductPlan(plan, nextPlan);
            if (firstPlan.blocksAccessed() < secondPlan.blocksAccessed()) {
                plan = firstPlan;
            } else {
                plan = secondPlan;
            }
        }

        plan = new SelectPlan(plan, data.predicate());

        plan = new ProjectPlan(plan, data.fieldNames());

        return plan;
    }
}
