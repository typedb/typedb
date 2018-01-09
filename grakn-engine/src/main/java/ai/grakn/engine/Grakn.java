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

package ai.grakn.engine;

import ai.grakn.bootup.PidRetriever;
import ai.grakn.engine.grakn_pid.GraknPid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Main class invoked by bash scripting
 *
 * @author Michele Orsi
 *
 */
public class Grakn {

    private static final Logger LOG = LoggerFactory.getLogger(Grakn.class);

    /**
     *
     * Invocation from class 'GraknProcess' in grakn-dist project
     *
     * @param args
     */
    public static void main(String[] args) {
        try {
            GraknPid pidFile = newPidFile_deleteOnExit();
            pidFile.createPidFile_deleteOnExit();

            // Start Engine
            GraknEngineServer graknEngineServer = new GraknCreator().instantiateGraknEngineServer(Runtime.getRuntime());
            graknEngineServer.start();
        } catch (Exception e) {
            LOG.error("An exception has occurred", e);
        }
    }

    private static GraknPid newPidFile_deleteOnExit() {
        long pid = new PidRetriever().getPid();
        return new GraknPid(pid);
    }
}

