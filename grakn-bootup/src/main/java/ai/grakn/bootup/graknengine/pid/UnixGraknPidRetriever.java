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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 *
 * A class capable of retrieving the process id of Grakn via `ps` command line tool
 *
 * @author Ganeshwara Herawan Hananda
 *
 */

public class UnixGraknPidRetriever implements GraknPidRetriever {
    public static final String psEfCommand = "ps -ef | ps -ef | grep \"ai.grakn.bootup.graknengine.Grakn\" | grep -v grep | awk '{print $2}'";
    public long getPid() {
        StringBuilder outputS = new StringBuilder();
        int exitValue = 1;

        Process p;
        try {
            p = Runtime.getRuntime().exec(new String[] { "/bin/sh", "-c", psEfCommand }, null, null);
            p.waitFor();
            exitValue = p.exitValue();

            if (exitValue == 0) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))){
                    String line;
                    while ((line = reader.readLine()) != null) {
                        outputS.append(line).append("\n");
                    }
                }
            } else {
                throw new RuntimeException("a non-zero exit code '" + exitValue + "'returned by the command '" + psEfCommand + "'");
            }
        } catch (InterruptedException | IOException e) {
            // DO NOTHING
        }

        String pidString = outputS.toString().trim();
        try {
            return Long.parseLong(pidString);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Couldn't get PID of Grakn. Received '" + pidString);
        }
    }
}
