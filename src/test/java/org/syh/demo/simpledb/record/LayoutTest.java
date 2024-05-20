package org.syh.demo.simpledb.record;

import org.junit.Test;

public class LayoutTest {
    @Test
    public void layoutTest() {
        Schema schema = new Schema();
        schema.addIntField("id");
        schema.addStringField("email", 42);
        schema.addStringField("phone", 11);
        Layout layout = new Layout(schema);
        for (String fieldName : layout.getSchema().fields()) {
            int offset = layout.getOffset(fieldName);
            System.out.println(fieldName + " has offset " + offset);
        }
    }
}
