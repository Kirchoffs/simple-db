package org.syh.demo.simpledb.parse;

import org.junit.Test;

public class LexerTest {
    @Test
    public void testLexer() throws Exception {
        Lexer lexer = new Lexer("SELECT target FROM source WHERE x = 42 AND y = 89");
        while (!lexer.isEnd()) {
            if (lexer.matchKeyword("SELECT")) {
                lexer.eatKeyword("SELECT");
                System.out.println("SELECT");
            } else if (lexer.matchKeyword("FROM")) {
                lexer.eatKeyword("FROM");
                System.out.println("FROM");
            } else if (lexer.matchKeyword("WHERE")) {
                lexer.eatKeyword("WHERE");
                System.out.println("WHERE");
            } else if (lexer.matchKeyword("AND")) {
                lexer.eatKeyword("AND");
                System.out.println("AND");
            } else if (lexer.matchId()) {
                System.out.println("Id: " + lexer.eatId());
            } else if (lexer.matchIntConstant()) {
                System.out.println("IntConstant: " + lexer.eatIntConstant());
            } else if (lexer.matchDelimiter('=')) {
                lexer.eatDelimiter('=');
                System.out.println("=");
            }
        }
    }
}
