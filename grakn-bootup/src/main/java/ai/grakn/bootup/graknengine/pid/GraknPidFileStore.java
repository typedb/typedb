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

package ai.grakn.bootup.graknengine.pid;

/*-
 * #%L
 * grakn-bootup
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.util.ErrorMessage;

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
        if (!pidFilePath.toFile().exists()) {
            String pidString = Long.toString(pid);
            try {
                Files.write(pidFilePath, pidString.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new GraknPidException(ErrorMessage.PID_ALREADY_EXISTS.getMessage(pidFilePath.toString()));
        }
    }
}
