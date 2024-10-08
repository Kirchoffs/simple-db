package org.syh.demo.simpledb.parse.data;

public class CreateIndexData {
    private String indexName;
    private String tableName;
    private String fieldName;

    public CreateIndexData(String indexName, String tableName, String fieldName) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.fieldName = fieldName;
    }

    public String indexName() {
        return indexName;
    }

    public String tableName() {
        return tableName;
    }

    public String fieldName() {
        return fieldName;
    }

    @Override
    public String toString() {
        return "CREATE INDEX " + indexName + " ON " + tableName + " (" + fieldName + ")";
    }
}
