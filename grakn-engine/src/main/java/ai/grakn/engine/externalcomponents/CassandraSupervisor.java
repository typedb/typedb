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

package ai.grakn.engine.externalcomponents;

import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.exception.GraknBackendException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

/**
 * Responsible for supervising cassandra.
 *
 * @author Ganeshwara Herawan Hananda
 */

public class CassandraSupervisor {
    private OperatingSystemCalls osCalls;
    private final Logger LOG = Logger.getLogger(CassandraSupervisor.class.getName());
    private final long WAIT_TIME_BETWEEN_ATTEMPT_MS = 2000;

    // commands
    private final String nodeToolCheckRunningCmd;
    private final String NODETOOL_RESPONSE_IF_RUNNING = "running";
    private final String cassandraStartCmd;
    private final String cassandraStartCmdSync;
    private final String cassandraPidFile;

    public CassandraSupervisor(GraknEngineConfig prop, OperatingSystemCalls osCalls, String baseWorkDir) {
        this.osCalls = osCalls;

        this.cassandraPidFile = prop.getProperty(GraknEngineConfig.STORAGE_CASSANDRA_PID_FILE);

        this.cassandraStartCmd = baseWorkDir + "bin/cassandra -p " + this.cassandraPidFile;
        this.cassandraStartCmdSync = baseWorkDir + "bin/cassandra -f -p " + this.cassandraPidFile;

        this.nodeToolCheckRunningCmd = baseWorkDir + "bin/nodetool statusthrift 2>/dev/null | tr -d '\\n\\r'";
    }

    public void startIfNotRunning() throws IOException, InterruptedException {
        LOG.info("checking if there exists a running grakn-cassandra process...");
        if (!isRunning()) {
            LOG.info("grakn-cassandra isn't yet running. attempting to start...");
            start();
        } else {
            LOG.info("found an existing grakn-cassandra process.");
        }
    }

    public void stopIfRunning() throws IOException, InterruptedException {
        LOG.info("checking if there exists a running grakn-cassandra process...");
        if (isRunning()) {
            LOG.info("a grakn-cassandra process found. attempting to stop...");
            stop();

            LOG.info("grakn-cassandra has been stopped.");
        } else {
            LOG.info("no grakn-cassandra process is found.");
        }
    }

    public boolean isRunning() throws IOException, InterruptedException {
        String[] cmd = new String[] { "sh", "-c", nodeToolCheckRunningCmd };
        Process nodeTool = osCalls.exec(cmd);
        int exitCode = nodeTool.waitFor();
        if (exitCode != 0) {
            throw GraknBackendException.operatingSystemCallException(String.join("", cmd), exitCode);
        }
        String lines = osCalls.readStdoutFromProcess(nodeTool);
        return lines.equals(NODETOOL_RESPONSE_IF_RUNNING);
    }

    public void start() throws IOException, InterruptedException {
        String[] cmd = new String[] { "sh", "-c", cassandraStartCmd };
        osCalls.execAndReturn(cmd);
        waitForCassandraStarted();
    }

    public Map.Entry<Process, CompletableFuture<Void>> startSync() throws IOException {
        try {
            String[] cmd = new String[] { "sh", "-c", cassandraStartCmdSync };
            LOG.info("starting cassandra...");
            Process p = osCalls.exec(cmd);

            CompletableFuture<Void> f = CompletableFuture.supplyAsync(() -> {
                try {
                    LOG.info("cassandra stopped.");
                    p.waitFor();
                    return null;
                } catch (InterruptedException e) {
                    throw GraknBackendException.cassandraStartException(e);
                }
            });

            return new HashMap.SimpleImmutableEntry<>(p, f);
        } catch (IOException e) {
            throw GraknBackendException.cassandraStartException(e);
        }
    }

    public void stop() throws IOException, InterruptedException {
        if (osCalls.fileExists(cassandraPidFile)) {
            int pid = osCalls.catPidFile(cassandraPidFile);
            boolean processRunning = osCalls.psP(pid) == 0;
            if (processRunning) {
                // process found, stop it
                String[] cmd = new String[] { "sh", "-c", "kill " + pid };
                osCalls.execAndReturn(cmd);
                waitForCassandraStopped();
            }
        }
    }

    public void waitForCassandraStarted() throws IOException, InterruptedException {
        final int MAX_CHECK_ATTEMPT = 15;
        int attempt = 0;
        while (attempt < MAX_CHECK_ATTEMPT) {
            if (isRunning()) {
                LOG.info("grakn-cassandra has been started successfully!");
                return;
            } else {
                LOG.info("grakn-cassandra has not yet started. will re-attempt the check...");
                attempt++;
                Thread.sleep(WAIT_TIME_BETWEEN_ATTEMPT_MS);
            }
        }
        LOG.info("unable to start grakn-cassandra!");
        throw GraknBackendException.cassandraStartException();
    }

    public void waitForCassandraStopped() throws IOException, InterruptedException {
        final int MAX_CHECK_ATTEMPT = 3;
        int attempt = 0;
        while (attempt < MAX_CHECK_ATTEMPT) {
            if (!isRunning()) {
                LOG.info("grakn-cassandra has been stopped successfully!");
                return;
            } else {
                LOG.info("grakn-cassandra has not been stopped. will re-attempt the check...");
                attempt++;
                Thread.sleep(WAIT_TIME_BETWEEN_ATTEMPT_MS);
            }
        }
        LOG.info("unable to stop grakn-cassandra!");
        throw GraknBackendException.cassandraStopException();
    }
}