package org.syh.demo.simpledb.parse.data;

import org.syh.demo.simpledb.record.FieldType;
import org.syh.demo.simpledb.record.Schema;

public class CreateTableData {
    private String tableName;
    private Schema schema;

    public CreateTableData(String tableName, Schema schema) {
        this.tableName = tableName;
        this.schema = schema;
    }

    public String tableName() {
        return tableName;
    }

    public Schema schema() {
        return schema;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("CREATE TABLE ");
        result.append(tableName).append(" (");
        for (String field : schema.fields()) {
            FieldType type = schema.getType(field);
            result.append(field).append(" ");
            if (type == FieldType.INTEGER) {
                result.append("INT");
            } else {
                result.append("VARCHAR(").append(schema.length(field)).append(")");
            }
            result.append(", ");
        }
        result.setLength(result.length() - 2);
        result.append(")");

        return result.toString();
    }
}
