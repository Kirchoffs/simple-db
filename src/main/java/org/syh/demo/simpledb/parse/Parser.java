package org.syh.demo.simpledb.parse;

import org.syh.demo.simpledb.record.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Parser {
    private Lexer lexer;

    public Parser(String s) {
        lexer = new Lexer(s);
    }

    public QueryData query() {
        lexer.eatKeyword("SELECT");
        List<String> fields = selectList();

        lexer.eatKeyword("FROM");
        Collection<String> tables = tableList();

        Predicate predicate = new Predicate();
        if (lexer.matchKeyword("WHERE")) {
            lexer.eatKeyword("WHERE");
            predicate = predicate();
        }

        return new QueryData(fields, tables, predicate);
    }

    public Object mutation() {
        if (lexer.matchKeyword("INSERT")) {
            return insert();
        } else if (lexer.matchKeyword("DELETE")) {
            return delete();
        } else if (lexer.matchKeyword("UPDATE")) {
            return update();
        } else {
            return create();
        }
    }

    private InsertData insert() {
        lexer.eatKeyword("INSERT");
        lexer.eatKeyword("INTO");
        String table = field();

        lexer.eatDelimiter('(');
        List<String> fields = selectList();
        lexer.eatDelimiter(')');

        lexer.eatKeyword("VALUES");
        lexer.eatDelimiter('(');
        List<Constant> values = constantList();
        lexer.eatDelimiter(')');

        return new InsertData(table, fields, values);
    }

    private DeleteData delete() {
        lexer.eatKeyword("DELETE");
        lexer.eatKeyword("FROM");
        String table = field();

        Predicate predicate = new Predicate();
        if (lexer.matchKeyword("WHERE")) {
            lexer.eatKeyword("WHERE");
            predicate = predicate();
        }

        return new DeleteData(table, predicate);
    }

    private UpdateData update() {
        lexer.eatKeyword("UPDATE");
        String table = field();

        lexer.eatKeyword("SET");
        String field = field();
        lexer.eatDelimiter('=');
        Expression value = expression();

        Predicate predicate = new Predicate();
        if (lexer.matchKeyword("WHERE")) {
            lexer.eatKeyword("WHERE");
            predicate = predicate();
        }
        return new UpdateData(table, field, value, predicate);
    }

    private Object create() {
        lexer.eatKeyword("CREATE");
        if (lexer.matchKeyword("TABLE")) {
            return createTable();
        } else if (lexer.matchKeyword("VIEW")) {
            return createView();
        } else {
            return createIndex();
        }
    }

    private CreateTableData createTable() {
        lexer.eatKeyword("TABLE");
        String table = field();

        lexer.eatDelimiter('(');
        Schema schema = fieldDefinitions();
        lexer.eatDelimiter(')');

        return new CreateTableData(table, schema);
    }

    private CreateViewData createView() {
        lexer.eatKeyword("VIEW");
        String view = field();

        lexer.eatKeyword("AS");
        QueryData queryData = query();

        return new CreateViewData(view, queryData);
    }

    private CreateIndexData createIndex() {
        lexer.eatKeyword("INDEX");
        String index = field();

        lexer.eatKeyword("ON");
        String table = field();
        lexer.eatDelimiter('(');
        String field = field();
        lexer.eatDelimiter(')');

        return new CreateIndexData(index, table, field);
    }

    private List<String> selectList() {
        List<String> fields = new ArrayList<>();
        fields.add(field());
        while (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');
            fields.add(field());
        }
        return fields;
    }

    private Collection<String> tableList() {
        Collection<String> tables = new ArrayList<>();
        tables.add(field());
        while (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');
            tables.add(field());
        }
        return tables;
    }

    private List<Constant> constantList() {
        List<Constant> constants = new ArrayList<>();
        constants.add(constant());
        while (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');
            constants.add(constant());
        }
        return constants;
    }

    private Schema fieldDefinitions() {
        Schema schema = fieldDefinition();
        while (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');
            schema.addAll(fieldDefinition());
        }
        return schema;
    }

    private Schema fieldDefinition() {
        String field = field();
        return fieldWithType(field);
    }

    private Schema fieldWithType(String field) {
        Schema schema = new Schema();

        if (lexer.matchKeyword("INT")) {
            lexer.eatKeyword("INT");
            schema.addIntField(field);
        } else {
            lexer.eatKeyword("VARCHAR");
            lexer.eatDelimiter('(');
            int length = lexer.eatIntConstant();
            lexer.eatDelimiter(')');
            schema.addStringField(field, length);
        }

        return schema;
    }

    private Predicate predicate() {
        Predicate pred = new Predicate(term());
        if (lexer.matchKeyword("AND")) {
            lexer.eatKeyword("AND");
            pred.conjoinWith(predicate());
        }
        return pred;
    }

    private Expression expression() {
        if (lexer.matchId()) {
            return new Expression(field());
        } else {
            return new Expression(constant());
        }
    }

    private Term term() {
        Expression lhs = expression();
        lexer.eatDelimiter('=');
        Expression rhs = expression();
        return new Term(lhs, rhs);
    }

    private String field() {
        return lexer.eatId();
    }

    private Constant constant() {
        if (lexer.matchStringConstant()) {
            return new Constant(lexer.eatStringConstant());
        } else {
            return new Constant(lexer.eatIntConstant());
        }
    }
}
