package org.syh.demo.simpledb.parse;

import org.junit.Assert;
import org.junit.Test;
import org.syh.demo.simpledb.parse.data.CreateIndexData;
import org.syh.demo.simpledb.parse.data.CreateTableData;
import org.syh.demo.simpledb.parse.data.CreateViewData;
import org.syh.demo.simpledb.parse.data.DeleteData;
import org.syh.demo.simpledb.parse.data.InsertData;
import org.syh.demo.simpledb.parse.data.QueryData;
import org.syh.demo.simpledb.parse.data.UpdateData;

public class ParserTest {
    @Test
    public void testParseQuery() {
        String sql = "SELECT target_field FROM target_table WHERE x = 42 AND y = '89'";
        Parser parser = new Parser(sql);
        QueryData queryData = parser.query();
        System.out.println(queryData);
        Assert.assertEquals(sql, queryData.toString());
    }

    @Test
    public void testParseInsert() {
        String sql = "INSERT INTO target_table (x, y) VALUES (42, '89')";
        Parser parser = new Parser(sql);
        InsertData insertData = (InsertData) parser.mutation();
        System.out.println(insertData);
        Assert.assertEquals(sql, insertData.toString());
    }

    @Test
    public void testParseDelete() {
        String sql = "DELETE FROM target_table WHERE x = 42 AND y = '89'";
        Parser parser = new Parser(sql);
        DeleteData deleteData = (DeleteData) parser.mutation();
        System.out.println(deleteData);
        Assert.assertEquals(sql, deleteData.toString());
    }

    @Test
    public void testParseUpdate() {
        String sql = "UPDATE target_table SET x = 42 WHERE x = 89 AND y = '42'";
        Parser parser = new Parser(sql);
        UpdateData updateData = (UpdateData) parser.mutation();
        System.out.println(updateData);
        Assert.assertEquals(sql, updateData.toString());
    }

    @Test
    public void testParseCreateTable() {
        String sql = "CREATE TABLE target_table (x INT, y VARCHAR(16))";
        Parser parser = new Parser(sql);
        CreateTableData createTableData = (CreateTableData) parser.mutation();
        System.out.println(createTableData);
        Assert.assertEquals(sql, createTableData.toString());
    }

    @Test
    public void testParseCreateView() {
        String sql = "CREATE VIEW target_view AS SELECT x, y FROM target_table WHERE x = 42 AND y = '89'";
        Parser parser = new Parser(sql);
        CreateViewData createViewData = (CreateViewData) parser.mutation();
        System.out.println(createViewData);
        Assert.assertEquals(sql, createViewData.toString());
    }

    @Test
    public void testParseCreateIndex() {
        String sql = "CREATE INDEX target_index ON target_table (x)";
        Parser parser = new Parser(sql);
        CreateIndexData createIndexData = (CreateIndexData) parser.mutation();
        System.out.println(createIndexData);
        Assert.assertEquals(sql, createIndexData.toString());
    }
}
