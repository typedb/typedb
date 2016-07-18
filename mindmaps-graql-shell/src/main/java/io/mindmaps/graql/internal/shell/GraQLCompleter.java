package io.mindmaps.graql.internal.shell;

import io.mindmaps.graql.internal.parser.GraqlLexer;
import jline.console.completer.Completer;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.Token;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * An autocompleter for Graql.
 * Provides a default 'complete' method that will filter results to only those that pass the Graql lexer
 */
interface GraqlCompleter extends Completer {

    @Override
    default int complete(String buffer, int cursor, List<CharSequence> candidates) {
        Optional<? extends Token> token = getCursorToken(buffer, cursor);
        getCandidates(buffer)
                .filter(s -> !token.isPresent() || s.startsWith(token.get().getText()))
                .forEach(candidates::add);


        return token.map(t -> {
            if (candidates.size() == 1 && candidates.get(0).equals(t.getText())) {
                // Add a space if the user is at the end of an unambiguous keyword
                candidates.clear();
                candidates.add(" ");
                return cursor;
            } else {
                return t.getStartIndex();
            }
        }).orElse(cursor);
    }

    static Optional<? extends Token> getCursorToken(String buffer, int cursor) {
        if (buffer == null) return Optional.empty();

        ANTLRInputStream input = new ANTLRInputStream(buffer);
        GraqlLexer lexer = new GraqlLexer(input);

        // Ignore syntax errors
        lexer.removeErrorListeners();
        lexer.addErrorListener(new BaseErrorListener());

        return lexer.getAllTokens().stream()
                .filter(t -> t.getStartIndex() <= cursor && t.getStopIndex() >= cursor - 1)
                .findFirst();
    }

    /**
     * @return all possible autocomplete candidates
     */
    Stream<String> getCandidates(String buffer);
}
