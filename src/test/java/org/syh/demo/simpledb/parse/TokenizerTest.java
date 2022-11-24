package org.syh.demo.simpledb.parse;

import org.junit.Test;

import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

public class TokenizerTest {
    @Test
    public void testStreamTokenizer() throws Exception {
        List<String> keywords = Arrays.asList(
            "select", "from", "where", "and",
            "insert", "into", "values", "delete", "update", "set",
            "create", "table", "int", "varchar", "view", "as", "index", "on"
        );

        String testString = "select target from table where x = 42 and y = 89";
        StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(testString));
        tokenizer.ordinaryChar('.');
        tokenizer.lowerCaseMode(true);

        while (tokenizer.nextToken() != StreamTokenizer.TT_EOF) {
            printCurrentToken(tokenizer, keywords);
        }
    }

    private void printCurrentToken(StreamTokenizer tokenizer, List<String> keywords) {
        if (tokenizer.ttype == StreamTokenizer.TT_NUMBER) {
            System.out.println("IntConstant " + (int) tokenizer.nval);
        } else if (tokenizer.ttype == StreamTokenizer.TT_WORD) {
            String word = tokenizer.sval;
            if (keywords.contains(word)) {
                System.out.println("Keyword " + word);
            } else {
                System.out.println("Id " + word);
            }
        } else if (tokenizer.ttype == '\'') {
            System.out.println("StringConstant " + tokenizer.sval);
        } else {
            System.out.println("Delimiter " + (char) tokenizer.ttype);
        }
    }
}
