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

import io.mindmaps.graql.api.shell.GraqlShell;
import jline.console.completer.Completer;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Completer that complete Graql shell commands
 */
public class ShellCommandCompleter implements Completer {

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates) {
        Set<String> commands = Stream.of(GraqlShell.COMMANDS)
                .filter(command -> command.startsWith(buffer))
                .collect(Collectors.toSet());

        if (commands.isEmpty()) {
            return cursor;
        } else {
            candidates.addAll(commands);
            return 0;
        }
    }
}
