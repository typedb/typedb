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

import java.lang.management.ManagementFactory;

/**
 *
 * A class capable of retrieving the process id of Grakn via `ps` command line tool
 *
 * @author Ganeshwara Herawan Hananda
 *
 */

public class UnixGraknPidRetriever implements GraknPidRetriever {
    public long getPid() {
        String[] pidAndHostnameString = ManagementFactory.getRuntimeMXBean().getName().split("@");
        String pidString = pidAndHostnameString[0];
        try {
            return Long.parseLong(pidString);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Couldn't get the PID of Grakn Engine. Received '" + pidString + "'");
        }
    }
}
