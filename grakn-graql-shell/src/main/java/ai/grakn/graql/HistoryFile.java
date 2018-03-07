/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.graql;

import jline.console.ConsoleReader;
import jline.console.history.FileHistory;

import java.io.File;
import java.io.IOException;

/**
 * Represents the history file used in {@link GraqlShell}. Will flush to disk when closed.
 *
 * @author Felix Chapman
 */
final class HistoryFile implements AutoCloseable {

    private final FileHistory jlineHistory;

    private HistoryFile(FileHistory jlineHistory) {
        this.jlineHistory = jlineHistory;
    }

    public static HistoryFile create(ConsoleReader consoleReader, String historyFilename) throws IOException {

        // Create history file
        File historyFile = new File(historyFilename);

        // The result of this method indicates if the file already existed, but that doesn't matter
        //noinspection ResultOfMethodCallIgnored
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
