/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.shell;

import ai.grakn.graql.GraqlShell;
import jline.console.completer.Completer;

import java.util.List;

/**
 * Completer that complete Graql shell commands
 *
 * @author Felix Chapman
 */
public class ShellCommandCompleter implements Completer {

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates) {
        GraqlShell.COMMANDS.stream()
                .filter(command -> command.startsWith(buffer))
                .forEach(candidates::add);

        return 0;
    }
}
