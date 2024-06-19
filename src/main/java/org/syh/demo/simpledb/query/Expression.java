package org.syh.demo.simpledb.query;

import org.syh.demo.simpledb.record.Schema;

public class Expression {
    private Constant val;
    private String fieldName;

    public Expression(Constant val) {
        this.val = val;
    }

    public Expression(String fieldName) {
        this.fieldName = fieldName;
    }

    public Constant evaluate(Scan scan) {
        if (val != null) {
            return val;
        } else {
            return scan.getVal(fieldName);
        }
    }

    public boolean isFieldName() {
        return fieldName != null;
    }

    public Constant asConstant() {
        return val;
    }

    public String asFieldName() {
        return fieldName;
    }

    public boolean appliesTo(Schema schema) {
        if (val != null) {
            return true;
        } else {
            return schema.hasField(fieldName);
        }
    }

    public String toString() {
        if (val != null) {
            return val.toString();
        } else {
            return fieldName;
        }
    }
}
