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

package grakn.core.server.bootup;

import grakn.core.util.ErrorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 *
 * A class responsible for managing the PID file of Engine
 *
 *
 */
public class EnginePidManager {
    private static final Logger LOG = LoggerFactory.getLogger(EnginePidManager.class);

    private Path pidFile;

    public EnginePidManager(Path pidFile) {
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
            throw new BootupException(ErrorMessage.COULD_NOT_GET_PID.getMessage(pidString), e);
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
            throw new BootupException(e);
        }
    }
}
