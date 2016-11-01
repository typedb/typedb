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

import io.mindmaps.graql.GraqlShell;
import jline.console.completer.Completer;
import mjson.Json;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.mindmaps.util.REST.RemoteShell.AUTOCOMPLETE_CANDIDATES;
import static io.mindmaps.util.REST.RemoteShell.AUTOCOMPLETE_CURSOR;

/**
 * An autocompleter for Graql.
 * Provides a default 'complete' method that will filter results to only those that pass the Graql lexer
 */
public class GraQLCompleter implements Completer {

    private final GraqlShell shell;

    public GraQLCompleter(GraqlShell shell) {
        this.shell = shell;
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates) {
        Json json = null;

        try {
            json = shell.getAutocompleteCandidates(buffer, cursor);
        } catch (InterruptedException | ExecutionException | IOException e) {
            e.printStackTrace();
        }

        json.at(AUTOCOMPLETE_CANDIDATES).asJsonList().forEach(candidate -> candidates.add(candidate.asString()));
        return json.at(AUTOCOMPLETE_CURSOR).asInteger();
    }
}
