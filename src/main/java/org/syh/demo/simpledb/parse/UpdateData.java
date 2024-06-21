package org.syh.demo.simpledb.parse;

public class UpdateData {
    private String tableName;
    private String field;
    private Expression value;
    private Predicate predicate;

    public UpdateData(String tableName, String field, Expression value, Predicate predicate) {
        this.tableName = tableName;
        this.field = field;
        this.value = value;
        this.predicate = predicate;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFields() {
        return field;
    }

    public Expression getValues() {
        return value;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    @Override
    public String toString() {
        return "UPDATE " + tableName + " SET " + field + " = " + value + " WHERE " + predicate;
    }
}
