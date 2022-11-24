package org.syh.demo.simpledb.jdbc.embedded;

import org.syh.demo.simpledb.jdbc.ResultSetMetaDataAdapter;
import org.syh.demo.simpledb.record.FieldType;
import org.syh.demo.simpledb.record.Schema;

public class EmbeddedResultSetMetaData extends ResultSetMetaDataAdapter {
    private Schema schema;

    public EmbeddedResultSetMetaData(Schema schema) {
        this.schema = schema;
    }

    @Override
    public int getColumnCount() {
        return schema.fields().size();
    }

    @Override
    public String getColumnName(int column) {
        return schema.fields().get(column - 1);
    }

    @Override
    public int getColumnType(int column) {
        String fieldName = getColumnName(column);
        return schema.getType(fieldName).getValue();
    }

    @Override
    public int getColumnDisplaySize(int column) {
        String fieldName = getColumnName(column);
        FieldType fieldType = schema.getType(fieldName);
        int fieldLength = fieldType == FieldType.INTEGER ? 6 : schema.length(fieldName);
        return Math.max(fieldLength, fieldName.length());
    }
}
