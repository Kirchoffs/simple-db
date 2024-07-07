package org.syh.demo.simpledb.parse;

import org.syh.demo.simpledb.plan.Plan;
import org.syh.demo.simpledb.query.Scan;
import org.syh.demo.simpledb.record.Schema;

public class Term {
    private Expression lhs;
    private Expression rhs;

    public Term(Expression lhs, Expression rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    public boolean isSatisfied(Scan scan) {
        Constant lhsVal = lhs.evaluate(scan);
        Constant rhsVal = rhs.evaluate(scan);
        return lhsVal.equals(rhsVal);
    }

    public int reductionFactor(Plan plan) {
        String lhsName, rhsName;
        if (lhs.isFieldName() && rhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            rhsName = rhs.asFieldName();
            return Math.max(plan.distinctValues(lhsName), plan.distinctValues(rhsName));
        } else if (lhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            return plan.distinctValues(lhsName);
        } else if (rhs.isFieldName()) {
            rhsName = rhs.asFieldName();
            return plan.distinctValues(rhsName);
        }

        if (lhs.asConstant().equals(rhs.asConstant())) {
            return 1;
        } else {
            return Integer.MAX_VALUE;
        }
    }

    public Constant equatesWithConstant(String fieldName) {
        if (lhs.isFieldName() && lhs.asFieldName().equals(fieldName) && !rhs.isFieldName()) {
            return rhs.asConstant();
        } else if (rhs.isFieldName() && rhs.asFieldName().equals(fieldName) && !lhs.isFieldName()) {
            return lhs.asConstant();
        } else {
            return null;
        }
    }

    public String equatesWithField(String fieldName) {
        if (lhs.isFieldName() && lhs.asFieldName().equals(fieldName) && rhs.isFieldName()) {
            return rhs.asFieldName();
        } else if (rhs.isFieldName() && rhs.asFieldName().equals(fieldName) && lhs.isFieldName()) {
            return lhs.asFieldName();
        } else {
            return null;
        }
    }

    public boolean appliesTo(Schema schema) {
        return lhs.appliesTo(schema) && rhs.appliesTo(schema);
    }

    public String toString() {
        return lhs.toString() + " = " + rhs.toString();
    }
}
