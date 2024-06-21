package org.syh.demo.simpledb.parse;

public class CreateIndexData {
    private String indexName;
    private String tableName;
    private String fieldName;

    public CreateIndexData(String indexName, String tableName, String fieldName) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.fieldName = fieldName;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Override
    public String toString() {
        return "CREATE INDEX " + indexName + " ON " + tableName + " (" + fieldName + ")";
    }
}
