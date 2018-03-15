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

package ai.grakn.graql.shell;

import com.google.common.base.StandardSystemProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Optional;

/**
 * Represents an external editor that a user can execute from the console for editing queries.
 *
 * @author Felix Chapman
 */
final class ExternalEditor {

    private static final String DEFAULT_EDITOR = "vim";
    private static final String TEMP_FILENAME = "/graql-tmp.gql";

    private final String editor;
    private final File tempFile;

    private ExternalEditor(String editor, File tempFile) {
        this.editor = editor;
        this.tempFile = tempFile;
    }

    // The result of createNewFile indicates if the file already existed, but that doesn't matter
    @SuppressWarnings("ResultOfMethodCallIgnored")
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public static ExternalEditor create() throws IOException {
        // Get preferred editor
        Map<String, String> env = System.getenv();
        String editor = Optional.ofNullable(env.get("EDITOR")).orElse(DEFAULT_EDITOR);

        File tempFile = new File(StandardSystemProperty.JAVA_IO_TMPDIR.value() + TEMP_FILENAME);

        tempFile.createNewFile();

        return new ExternalEditor(editor, tempFile);
    }

    /**
     * load the user's preferred editor to edit a query
     *
     * @return the string written to the editor
     */
    public String execute() throws IOException, InterruptedException {
        // Run the editor, pipe input into and out of tty so we can provide the input/output to the editor via Graql
        ProcessBuilder builder = new ProcessBuilder(
                "/bin/bash",
                "-c",
                editor + " </dev/tty >/dev/tty " + tempFile.getAbsolutePath()
        );

        // Wait for user to finish editing
        builder.start().waitFor();

        return String.join("\n", Files.readAllLines(tempFile.toPath()));
    }
}
