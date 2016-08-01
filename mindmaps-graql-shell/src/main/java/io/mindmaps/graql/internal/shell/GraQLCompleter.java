/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

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
