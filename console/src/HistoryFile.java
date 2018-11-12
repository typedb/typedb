/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.console;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jline.console.ConsoleReader;
import jline.console.history.FileHistory;

import java.io.File;
import java.io.IOException;

/**
 * Represents the history file used in {@link GraqlShell}. Will flush to disk when closed.
 *
 */
final class HistoryFile implements AutoCloseable {

    private final FileHistory jlineHistory;

    private HistoryFile(FileHistory jlineHistory) {
        this.jlineHistory = jlineHistory;
    }

    // The result of createNewFile indicates if the file already existed, but that doesn't matter
    @SuppressWarnings("ResultOfMethodCallIgnored")
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public static HistoryFile create(ConsoleReader consoleReader, String historyFilename) throws IOException {

        // Create history file
        File historyFile = new File(historyFilename);

        historyFile.createNewFile();

        FileHistory jlineHistory = new FileHistory(historyFile);

        consoleReader.setHistory(jlineHistory);

        // Make sure history is saved on shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                jlineHistory.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));

        return new HistoryFile(jlineHistory);
    }

    @Override
    public void close() throws IOException {
        jlineHistory.flush();
    }
}
