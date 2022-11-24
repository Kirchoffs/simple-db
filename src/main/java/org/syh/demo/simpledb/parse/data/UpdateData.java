package org.syh.demo.simpledb.parse.data;

import org.syh.demo.simpledb.parse.models.Expression;
import org.syh.demo.simpledb.parse.models.Predicate;

public class UpdateData {
    private String tableName;
    private String fieldName;
    private Expression value;
    private Predicate predicate;

    public UpdateData(String tableName, String fieldName, Expression value, Predicate predicate) {
        this.tableName = tableName;
        this.fieldName = fieldName;
        this.value = value;
        this.predicate = predicate;
    }

    public String tableName() {
        return tableName;
    }

    public String fieldName() {
        return fieldName;
    }

    public Expression value() {
        return value;
    }

    public Predicate predicate() {
        return predicate;
    }

    @Override
    public String toString() {
        return "UPDATE " + tableName + " SET " + fieldName + " = " + value + " WHERE " + predicate;
    }
}
