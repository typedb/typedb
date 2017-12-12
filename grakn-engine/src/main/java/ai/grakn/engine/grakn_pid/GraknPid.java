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

package ai.grakn.engine.grakn_pid;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 * A class which manages grakn engine's PID
 *
 * @author Ganeshwara Herawan Hananda
 *
 */
public class GraknPid {
    public static final Path GRAKN_PID_FILE_PATH = Paths.get(File.separator,"tmp","grakn.pid");
    private final long pid;

    public GraknPid(long pid) {
        this.pid = pid;
    }

    public void createPidFile_deleteOnExit() {
        attemptToWritePidFile(pid, this.GRAKN_PID_FILE_PATH);
        deletePidFileOnExit();
    }

    private void deletePidFileOnExit() {
        this.GRAKN_PID_FILE_PATH.toFile().deleteOnExit();
    }

    private void attemptToWritePidFile(long pid, Path pidFilePath) {
        if (!pidFilePath.toFile().exists()) {
            String pidString = Long.toString(pid);
            try {
                Files.write(pidFilePath, pidString.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new PidFileAlreadyExistsException(pidFilePath);
        }
    }
}
