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

import java.io.IOException;
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
    private final String cassandraStopCmd;

    public CassandraSupervisor(OperatingSystemCalls osCalls, String baseWorkDir) {
        this.osCalls = osCalls;

        this.cassandraStartCmd = baseWorkDir + "bin/cassandra -p /tmp/grakn-cassandra.pid";
        this.cassandraStopCmd = baseWorkDir + "bin/grakn-cassandra.sh stop";
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
        Process nodeTool = osCalls.exec(new String[]{ "sh", "-c", nodeToolCheckRunningCmd});
        int status = nodeTool.waitFor();
        System.out.println("== " + nodeToolCheckRunningCmd + " / " + status + " ==");
        if (status != 0) throw new RuntimeException("unable to run nodetool - " + nodeToolCheckRunningCmd);
        String lines = osCalls.readStdoutFromProcess(nodeTool);
        return lines.equals(NODETOOL_RESPONSE_IF_RUNNING);
    }

    public void start() throws IOException, InterruptedException {
        Process startCassandra = osCalls.exec(new String[]{ "sh", "-c", cassandraStartCmd });
        int status = startCassandra.waitFor();
        System.out.println("== " + cassandraStartCmd + " / " + status + " ==");
        if (status != 0) throw new RuntimeException("unable to start cassandra - " + cassandraStartCmd);
        waitForCassandraStarted();
    }

    public void stop2() throws IOException, InterruptedException {
        Process stopCassandra = osCalls.exec(new String[]{ "sh", "-c", cassandraStopCmd });
        int status = stopCassandra.waitFor();
        System.out.println("== " + cassandraStopCmd + " / " + status + " ==");
        if (status != 0) throw new RuntimeException("unable to stop cassandra - " + cassandraStopCmd);
        waitForCassandraStopped();
    }

    public void stop() throws IOException, InterruptedException {
        if (osCalls.fileExists("/tmp/grakn-cassandra.pid")) {
            int pid = osCalls.catPidFile("/tmp/grakn-cassandra.pid");
            boolean processRunning = osCalls.psP(pid) == 0;
            if (processRunning) {
                // process found, stop it
                Process kill = Runtime.getRuntime().exec(new String[]{"sh", "-c", "kill " + pid});
                int status = kill.waitFor();
                if (status != 0) {
                    throw new RuntimeException("unable to stop cassandra - " + cassandraStopCmd);
                }
            }
        }
    }

    private void waitForCassandraStarted() throws IOException, InterruptedException {
        final int MAX_CHECK_ATTEMPT = 3;
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
        throw new ExternalComponentException("unable to start grakn-cassandra!");
    }

    private void waitForCassandraStopped() throws IOException, InterruptedException {
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
        throw new ExternalComponentException("unable to stop grakn-cassandra!");
    }
}