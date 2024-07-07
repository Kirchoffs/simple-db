package org.syh.demo.simpledb.parse;

import org.syh.demo.simpledb.parse.exceptions.BadSyntaxException;

import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;

public class Lexer {
    private Collection<String> keywords;
    private StreamTokenizer tokenizer;

    public Lexer(String input) {
        initKeywords();
        tokenizer = new StreamTokenizer(new StringReader(input));
        tokenizer.ordinaryChar('.');
        tokenizer.wordChars('_', '_');
        tokenizer.lowerCaseMode(true);
        nextToken();
    }

    public boolean matchDelimiter(char ch) {
        return ch == (char) tokenizer.ttype;
    }

    public boolean matchIntConstant() {
        return tokenizer.ttype == StreamTokenizer.TT_NUMBER;
    }

    public boolean matchStringConstant() {
        return '\'' == (char) tokenizer.ttype;
    }

    public boolean matchKeyword(String word) {
        word = word.toLowerCase();
        return tokenizer.ttype == StreamTokenizer.TT_WORD && tokenizer.sval.equals(word);
    }

    public boolean matchId() {
        return tokenizer.ttype == StreamTokenizer.TT_WORD && !keywords.contains(tokenizer.sval.toUpperCase());
    }

    public void eatDelimiter(char ch) {
        if (!matchDelimiter(ch)) {
            throw new BadSyntaxException();
        }
        nextToken();
    }

    public int eatIntConstant() {
        if (!matchIntConstant()) {
            throw new BadSyntaxException();
        }
        int i = (int) tokenizer.nval;
        nextToken();
        return i;
    }

    public String eatStringConstant() {
        if (!matchStringConstant()) {
            throw new BadSyntaxException();
        }
        String s = tokenizer.sval;
        nextToken();
        return s;
    }

    public void eatKeyword(String word) {
        if (!matchKeyword(word)) {
            throw new BadSyntaxException();
        }
        nextToken();
    }

    public String eatId() {
        if (!matchId()) {
            throw new BadSyntaxException();
        }
        String s = tokenizer.sval;
        nextToken();
        return s;
    }

    public boolean isEnd() {
        return tokenizer.ttype == StreamTokenizer.TT_EOF;
    }

    private void initKeywords() {
        keywords = Arrays.asList(
            "SELECT", "FROM", "WHERE", "AND",
            "INSERT", "INTO", "VALUES", "DELETE", "UPDATE", "SET",
            "CREATE", "TABLE", "INT", "VARCHAR", "VIEW", "AS", "INDEX", "ON"
        );
    }

    private void nextToken() {
        try {
            tokenizer.nextToken();
        } catch (Exception e) {
            throw new BadSyntaxException();
        }
    }
}
