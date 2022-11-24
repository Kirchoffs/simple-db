package org.syh.demo.simpledb.metadata;

import org.syh.demo.simpledb.index.Index;
import org.syh.demo.simpledb.index.btree.BTreeIndex;
import org.syh.demo.simpledb.record.FieldType;
import org.syh.demo.simpledb.record.Layout;
import org.syh.demo.simpledb.record.Schema;
import org.syh.demo.simpledb.transaction.Transaction;

public class IndexInfo {
    private String indexName;
    private String fieldName;
    private Transaction tx;
    private Schema tableSchema;
    private Layout indexLayout;
    private StatInfo statInfo;

    public IndexInfo(String indexName, String fieldName, Schema tableSchema, Transaction tx, StatInfo statInfo) {
        this.indexName = indexName;
        this.fieldName = fieldName;
        this.tx = tx;
        this.tableSchema = tableSchema;
        this.indexLayout = createIndexLayout();
        this.statInfo = statInfo;
    }

    public Index open() {
        return new BTreeIndex(tx, indexName, indexLayout);
    }

    public int getBlockAccessed() {
        int rpb = tx.getBlockSize() / indexLayout.slotSize();
        int numBlocks = statInfo.recordsOutput() / rpb;
        return -1;
    }

    private Layout createIndexLayout() {
        Schema schema = new Schema();
        schema.addIntField("blockNum");
        schema.addIntField("slot");
        if (tableSchema.getType(fieldName) == FieldType.INTEGER) {
            schema.addIntField("dataVal");
        } else {
            schema.addStringField("dataVal", tableSchema.length(fieldName));
        }
        return new Layout(schema);
    }

    public int getRecordsOutput() {
        return statInfo.recordsOutput() / statInfo.distinctValues(fieldName);
    }

    public int getDistinctValues(String fieldName) {
        return this.fieldName.equals(fieldName) ? 1 : statInfo.distinctValues(fieldName);
    }
}
