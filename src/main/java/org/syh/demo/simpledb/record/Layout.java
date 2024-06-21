package org.syh.demo.simpledb.record;

import java.util.HashMap;
import java.util.Map;

public class Layout {
    private Schema schema;
    private Map<String, Integer> offsets;
    private int slotSize;

    public Layout(Schema schema) {
        this.schema = schema;
        offsets = new HashMap<>();
        int pos = Integer.BYTES;
        for (String field : schema.fields()) {
            offsets.put(field, pos);
            pos += lengthInBytes(field);
        }
        slotSize = pos;
    }

    public Layout(Schema schema, Map<String, Integer> offsets, int slotSize) {
        this.schema = schema;
        this.offsets = offsets;
        this.slotSize = slotSize;
    }

    public Schema getSchema() {
        return schema;
    }

    public int getOffset(String field) {
        return offsets.get(field);
    }

    public int getSlotSize() {
        return slotSize;
    }

    private int lengthInBytes(String field) {
        FieldType type = schema.type(field);
        int length = schema.length(field);
        if (type == FieldType.INTEGER) {
            return Integer.BYTES;
        } else {
            return length;
        }
    }
}
