package org.syh.demo.simpledb.record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Schema {
    private List<String> fields;
    private Map<String, FieldInfo> info;

    public Schema() {
        fields = new ArrayList<>();
        info = new HashMap<>();
    }

    public void addField(String fieldName, FieldType type, int length) {
        fields.add(fieldName);
        info.put(fieldName, new FieldInfo(type, length));
    }

    public void addIntField(String fieldName) {
        addField(fieldName, FieldType.INTEGER, 0);
    }

    public void addStringField(String fieldName, int length) {
        addField(fieldName, FieldType.VARCHAR, length);
    }

    public void add(String fieldName, Schema schema) {
        FieldType type = schema.getType(fieldName);
        int length = schema.length(fieldName);
        addField(fieldName, type, length);
    }

    public void addAll(Schema schema) {
        for (String fieldName : schema.fields()) {
            add(fieldName, schema);
        }
    }

    public List<String> fields() {
        return fields;
    }

    public boolean hasField(String fieldName) {
        return fields.contains(fieldName);
    }

    public FieldType getType(String fieldName) {
        return info.get(fieldName).type;
    }

    public int length(String fieldName) {
        return info.get(fieldName).length;
    }

    class FieldInfo {
        FieldType type;
        int length;

        FieldInfo(FieldType type, int length) {
            this.type = type;
            this.length = length;
        }
    }
}
