package org.syh.demo.simpledb.metadata;

import org.syh.demo.simpledb.index.Index;
import org.syh.demo.simpledb.record.Layout;
import org.syh.demo.simpledb.record.Schema;
import org.syh.demo.simpledb.transaction.Transaction;

import static java.sql.Types.INTEGER;

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
        return null;
    }

    public int blockAccessed() {
        int rpb = tx.getBlockSize() / indexLayout.getSlotSize();
        int numBlocks = statInfo.recordsOutput() / rpb;
        return -1;
    }

    private Layout createIndexLayout() {
        Schema schema = new Schema();
        schema.addIntField("blockNum");
        schema.addIntField("slot");
        if (tableSchema.type(fieldName) == INTEGER) {
            schema.addIntField("value");
        } else {
            schema.addStringField("value", tableSchema.length(fieldName));
        }
        return new Layout(schema);
    }
}
