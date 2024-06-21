package org.syh.demo.simpledb.record;

public enum FieldType {
    INTEGER(1),
    VARCHAR(2);

    private int value;

    FieldType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static FieldType fromValue(int value) {
        for (FieldType type : FieldType.values()) {
            if (type.getValue() == value) {
                return type;
            }
        }
        return null;
    }
}
