package org.syh.demo.simpledb.parse.data;

import org.syh.demo.simpledb.parse.Constant;
import org.syh.demo.simpledb.utils.Pair;

import java.util.ArrayList;
import java.util.List;

public class InsertData {
    private String tableName;
    private List<String> fields;
    private List<Constant> values;
    private List<Pair<String, Constant>> fieldValues;

    public InsertData(String tableName, List<String> fields, List<Constant> values) {
        this.tableName = tableName;
        this.fields = fields;
        this.values = values;
        this.fieldValues = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            fieldValues.add(new Pair<>(fields.get(i), values.get(i)));
        }
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getFields() {
        return fields;
    }

    public List<Constant> getValues() {
        return values;
    }

    public List<Pair<String, Constant>> getFieldValues() {
        return fieldValues;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("INSERT INTO ");
        result.append(tableName).append(" (");
        for (String field : fields) {
            result.append(field).append(", ");
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
