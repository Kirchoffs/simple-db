package org.syh.demo.simpledb.parse.data;

import org.syh.demo.simpledb.parse.models.Constant;
import org.syh.demo.simpledb.utils.Pair;

import java.util.ArrayList;
import java.util.List;

public class InsertData {
    private String tableName;
    private List<String> fieldNames;
    private List<Constant> values;
    private List<Pair<String, Constant>> fields;

    public InsertData(String tableName, List<String> fieldNames, List<Constant> values) {
        this.tableName = tableName;
        this.fieldNames = fieldNames;
        this.values = values;
        this.fields = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            fields.add(new Pair<>(fieldNames.get(i), values.get(i)));
        }
    }

    public String tableName() {
        return tableName;
    }

    public List<String> fieldNames() {
        return fieldNames;
    }

    public List<Constant> values() {
        return values;
    }

    public List<Pair<String, Constant>> fields() {
        return fields;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("INSERT INTO ");
        result.append(tableName).append(" (");
        for (String fieldName : fieldNames) {
            result.append(fieldName).append(", ");
        }
        result.setLength(result.length() - 2);
        result.append(") ");

        result.append("VALUES (");
        for (Constant value : values) {
            result.append(value).append(", ");
        }
        result.setLength(result.length() - 2);
        result.append(")");

        return result.toString();
    }
}
