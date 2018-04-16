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

package ai.grakn.bootup.graknengine;

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

import ai.grakn.GraknSystemProperty;
import ai.grakn.bootup.graknengine.pid.GraknPidManager;
import ai.grakn.bootup.graknengine.pid.GraknPidManagerFactory;
import ai.grakn.engine.GraknEngineServerFactory;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.util.ErrorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 *
 * Main class invoked by bash scripting.
 *
 * NOTE: The class name is shown when a user is running the 'jps' command. Therefore please keep the class name "Grakn".
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
        Thread.setDefaultUncaughtExceptionHandler(newUncaughtExceptionHandler(LOG));

        try {
            String graknPidFileProperty = Optional.ofNullable(GraknSystemProperty.GRAKN_PID_FILE.value())
                    .orElseThrow(() -> new RuntimeException(ErrorMessage.GRAKN_PIDFILE_SYSTEM_PROPERTY_UNDEFINED.getMessage()));
            Path pidfile = Paths.get(graknPidFileProperty);
            GraknPidManager graknPidManager = GraknPidManagerFactory.newGraknPidManagerForUnixOS(pidfile);
            graknPidManager.trackGraknPid();

            // Start Engine
            GraknEngineServer graknEngineServer = GraknEngineServerFactory.createGraknEngineServer();
            graknEngineServer.start();
        } catch (IOException e) {
            LOG.error(ErrorMessage.UNCAUGHT_EXCEPTION.getMessage(), e);
        }
    }

    private static Thread.UncaughtExceptionHandler newUncaughtExceptionHandler(Logger logger) {
        return (Thread t, Throwable e) -> logger.error(ErrorMessage.UNCAUGHT_EXCEPTION.getMessage(t.getName()), e);
    }
}

