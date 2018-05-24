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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.bootup.graknengine.pid;

import ai.grakn.util.ErrorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 *
 * A class which manages grakn engine's PID
 *
 * @author Ganeshwara Herawan Hananda
 *
 */
public class GraknPidFileStore implements GraknPidStore {
    private static final Logger LOG = LoggerFactory.getLogger(GraknPidFileStore.class);

    private final Path pidFilePath;

    public GraknPidFileStore(Path pidFilePath) {
        this.pidFilePath = pidFilePath;
    }

    @Override
    public void trackGraknPid(long graknPid) {
        attemptToWritePidFile(graknPid, this.pidFilePath);
        deletePidFileOnExit();
    }

    private void deletePidFileOnExit() {
        this.pidFilePath.toFile().deleteOnExit();
    }

    private void attemptToWritePidFile(long pid, Path pidFilePath) {
        if (pidFilePath.toFile().exists()) {
            LOG.warn(ErrorMessage.PID_ALREADY_EXISTS.getMessage(pidFilePath.toString()));
        }
        String pidString = Long.toString(pid);
        try {
            Files.write(pidFilePath, pidString.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}