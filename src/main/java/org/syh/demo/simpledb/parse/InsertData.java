package org.syh.demo.simpledb.parse;

import java.util.List;

public class InsertData {
    private String tableName;
    private List<String> fields;
    private List<Constant> values;

    public InsertData(String tableName, List<String> fields, List<Constant> values) {
        this.tableName = tableName;
        this.fields = fields;
        this.values = values;
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
