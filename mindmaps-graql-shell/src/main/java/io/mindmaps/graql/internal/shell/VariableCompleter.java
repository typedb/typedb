package io.mindmaps.graql.internal.shell;

import io.mindmaps.graql.internal.parser.GraqlLexer;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.Token;

import java.util.List;
import java.util.stream.Stream;

/**
 * Completer that searches the query for any variables to autocomplete
 */
public class VariableCompleter implements GraqlCompleter {

    @Override
    public Stream<String> getCandidates(String buffer) {
        ANTLRInputStream input = new ANTLRInputStream(buffer);
        GraqlLexer lexer = new GraqlLexer(input);

        // Ignore syntax errors
        lexer.removeErrorListeners();
        lexer.addErrorListener(new BaseErrorListener());

        List<? extends Token> allTokens = lexer.getAllTokens();
        if (allTokens.size() > 0) allTokens.remove(allTokens.size() - 1);
        return allTokens.stream()
                .filter(t -> t.getType() == GraqlLexer.VARIABLE)
                .map(Token::getText);
    }
}