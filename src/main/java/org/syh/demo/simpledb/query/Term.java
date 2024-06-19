package org.syh.demo.simpledb.query;

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
        return lhs.toString() + "=" + rhs.toString();
    }
}
