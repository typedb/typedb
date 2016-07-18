package io.mindmaps.graql.internal.shell;

import io.mindmaps.graql.internal.parser.GraqlLexer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;

/**
 * Completer that fills in any possible Graql keywords
 */
public class KeywordCompleter implements GraqlCompleter {

    private final Collection<String> autocomplete = new ArrayList<>();

    /**
     * Create a new keyword completer, populating keywords using the Graql lexer
     */
    public KeywordCompleter() {
        for (int i = 1; GraqlLexer.VOCABULARY.getLiteralName(i) != null; i ++) {
            String name = GraqlLexer.VOCABULARY.getLiteralName(i);
            autocomplete.add(name.replaceAll("'", ""));
        }
    }

    @Override
    public Stream<String> getCandidates(String buffer) {
        return autocomplete.stream();
    }
}
