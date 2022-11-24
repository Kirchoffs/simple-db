package org.syh.demo.simpledb.parse.data;

import org.syh.demo.simpledb.parse.models.Predicate;

import java.util.Collection;
import java.util.List;

public class QueryData {
    private List<String> fieldNames;
    private Collection<String> tableNames;
    private Predicate predicate;

    public QueryData(List<String> fieldNames, Collection<String> tableNames, Predicate predicate) {
        this.fieldNames = fieldNames;
        this.tableNames = tableNames;
        this.predicate = predicate;
    }

    public List<String> fieldNames() {
        return fieldNames;
    }

    public Collection<String> tableNames() {
        return tableNames;
    }

    public Predicate predicate() {
        return predicate;
    }

    public String toString() {
        StringBuilder result = new StringBuilder("SELECT ");
        for (String fieldName : fieldNames) {
            result.append(fieldName).append(", ");
        }
        result.setLength(result.length() - 2);

        result.append(" FROM ");
        for (String tableName : tableNames) {
            result.append(tableName).append(", ");
        }
        result.setLength(result.length() - 2);

        String predicateString = predicate.toString();
        if (!predicateString.equals("")) {
            result.append(" WHERE ").append(predicateString);
        }

        return result.toString();
    }
}
