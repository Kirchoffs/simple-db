package org.syh.demo.simpledb.parse.data;

import org.syh.demo.simpledb.parse.Predicate;

import java.util.Collection;
import java.util.List;

public class QueryData {
    private List<String> fields;
    private Collection<String> tables;
    private Predicate predicate;

    public QueryData(List<String> fields, Collection<String> tables, Predicate predicate) {
        this.fields = fields;
        this.tables = tables;
        this.predicate = predicate;
    }

    public List<String> fields() {
        return fields;
    }

    public Collection<String> tables() {
        return tables;
    }

    public Predicate predicate() {
        return predicate;
    }

    public String toString() {
        StringBuilder result = new StringBuilder("SELECT ");
        for (String fieldName : fields) {
            result.append(fieldName).append(", ");
        }
        result.setLength(result.length() - 2);

        result.append(" FROM ");
        for (String tableName : tables) {
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
