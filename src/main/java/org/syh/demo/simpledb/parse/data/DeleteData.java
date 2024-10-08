package org.syh.demo.simpledb.parse.data;

import org.syh.demo.simpledb.parse.models.Predicate;

public class DeleteData {
    private String tableName;
    private Predicate predicate;

    public DeleteData(String tableName, Predicate predicate) {
        this.tableName = tableName;
        this.predicate = predicate;
    }

    public String tableName() {
        return tableName;
    }

    public Predicate predicate() {
        return predicate;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("DELETE FROM ");
        result.append(tableName);

        if (!tableName.isEmpty()) {
            result.append(" WHERE ");
            result.append(predicate);
        }

        return result.toString();
    }
}
