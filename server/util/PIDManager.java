/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.server.util;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.kb.server.exception.GraknServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A class responsible for managing the PID file of Grakn Server
 */
public class PIDManager {
    private static final Logger LOG = LoggerFactory.getLogger(PIDManager.class);

    private Path pidFile;

    public PIDManager(Path pidFile) {
        this.pidFile = pidFile;
    }

    public void trackGraknPid() {
        long pid = getPid();
        trackGraknPid(pid);
    }

    private long getPid() {
        String[] pidAndHostnameString = ManagementFactory.getRuntimeMXBean().getName().split("@");
        String pidString = pidAndHostnameString[0];
        try {
            return Long.parseLong(pidString);
        } catch (NumberFormatException e) {
            throw GraknServerException.invalidPIDException(pidString);
        }
    }

    private void trackGraknPid(long graknPid) {
        attemptToWritePidFile(graknPid, this.pidFile);
        deletePidFileOnExit();
    }

    private void deletePidFileOnExit() {
        this.pidFile.toFile().deleteOnExit();
    }

    private void attemptToWritePidFile(long pid, Path pidFilePath) {
        if (pidFilePath.toFile().exists()) {
            LOG.warn(ErrorMessage.PID_ALREADY_EXISTS.getMessage(pidFilePath.toString()));
        }
        String pidString = Long.toString(pid);
        try {
            Files.write(pidFilePath, pidString.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw GraknServerException.fileWriteException(pidFilePath.toString());
        }
    }
}
