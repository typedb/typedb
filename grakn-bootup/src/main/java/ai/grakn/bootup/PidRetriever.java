/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
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

package ai.grakn.bootup;

/**
 *
 * Main class invoked by bash scripting
 *
 * @author Michele Orsi
 *
 */
public class PidRetriever extends AbstractProcessHandler {
    static final String getPidCommand = "ps -ef | grep \"ai.grakn.engine.Grakn\" | grep -v grep | awk '{print $2}'";

    public long getPid() {
        OutputCommand execution = executeAndWait(new String[] { "/bin/sh", "-c", getPidCommand}, null, null);

        if (execution.succes()) {
            try {
                long pid = Long.parseLong(execution.output);
                return pid;
            } catch (NumberFormatException e) {
                throw new RuntimeException("Couldn't get Grakn process id. Received '" + execution.output + "'");
            }
        } else {
            throw new RuntimeException("Unable to retrieve process id. The Operating System returned exit code: " + execution.exitStatus);
        }
    }
}
