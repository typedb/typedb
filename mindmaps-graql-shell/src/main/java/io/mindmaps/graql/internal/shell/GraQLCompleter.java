package io.mindmaps.graql.internal.shell;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.graql.api.parser.Autocomplete;
import jline.console.completer.Completer;

import java.util.List;

/**
 * An autocompleter for Graql.
 * Provides a default 'complete' method that will filter results to only those that pass the Graql lexer
 */
public class GraQLCompleter implements Completer {

    private final MindmapsGraph graph;

    public GraQLCompleter(MindmapsGraph graph) {
        this.graph = graph;
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates) {
        Autocomplete autocomplete = Autocomplete.create(graph.newTransaction(), buffer, cursor);
        candidates.addAll(autocomplete.getCandidates());
        return autocomplete.getCursorPosition();
    }
}
