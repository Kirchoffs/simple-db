package org.syh.demo.simpledb.jdbc.network.remote.impl;

import org.syh.demo.simpledb.jdbc.network.remote.RemoteResultSetMetaData;
import org.syh.demo.simpledb.record.FieldType;
import org.syh.demo.simpledb.record.Schema;

import java.rmi.RemoteException;
import java.util.List;

public class RemoteResultSetMetaDataImpl implements RemoteResultSetMetaData {
    private Schema schema;
    private List<String> fields;

    public RemoteResultSetMetaDataImpl(Schema schema) throws RemoteException {
        this.schema = schema;
        fields = schema.fields();
        for (String field : schema.fields()) {
            fields.add(field);
        }
    }

    @Override
    public int getColumnCount() throws RemoteException {
        return fields.size();
    }

    @Override
    public String getColumnName(int column) throws RemoteException {
        return fields.get(column - 1);
    }

    @Override
    public int getColumnType(int column) throws RemoteException {
        String fieldName = getColumnName(column);
        return schema.getType(fieldName).getValue();
    }

    @Override
    public int getColumnDisplaySize(int column) throws RemoteException {
        String fieldName = getColumnName(column);
        FieldType fieldType = schema.getType(fieldName);
        int fieldLength = fieldType == FieldType.INTEGER ? 6 : schema.length(fieldName);
        return Math.max(fieldLength, fieldName.length());
    }
}
