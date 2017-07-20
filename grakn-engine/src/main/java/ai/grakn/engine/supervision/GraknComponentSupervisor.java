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

package ai.grakn.engine.supervision;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Responsible for supervising cassandra and redis processes.
 *
 * @author Ganeshwara Herawan Hananda
 */

public class GraknComponentSupervisor {
  private final Logger LOG = Logger.getLogger(GraknComponentSupervisor.class.getName());
  private final String CASSANDRA_FULL_PATH = "bin/cassandra"; // TODO: this exe shouldn't even be exposed anymore
  private final String CASSANDRA_PID_FILE = "/tmp/grakn-cassandra.pid";
  private final long WAIT_BETWEEN_ATTEMPT_MS = 2000;
  private OperatingSystemCalls osCalls = new OperatingSystemCalls();

  public void startCassandraIfNotExists() throws MalformedPidFileException, IOException, InterruptedException {
    LOG.info("checking if there exists a running grakn-cassandra process...");
    if (!isCassandraRunning()) {
      LOG.info("grakn-cassandra isn't yet running. attempting to start...");
      startCassandra();
      waitForCassandraStarted();

    } else {
      LOG.info("found an existing grakn-cassandra process.");
    }
  }

  public void stopCassandraIfRunning() throws MalformedPidFileException, IOException, InterruptedException {
    LOG.info("checking if there exists a running grakn-cassandra process...");
    if (isCassandraRunning()) {
      LOG.info("a grakn-cassandra process found. attempting to stop...");
      stopCassandra();
      waitForCassandraStopped();

      LOG.info("grakn-cassandra has been stopped.");
    }
    else {
      LOG.info("no grakn-cassandra process is found.");
    }
  }

  public boolean isCassandraRunning() throws MalformedPidFileException, IOException, InterruptedException {
    if (osCalls.fileExists(CASSANDRA_PID_FILE)) {
      int pid = osCalls.catPidFile(CASSANDRA_PID_FILE);
      boolean cassandraProcessFound = osCalls.psP(pid) == 0;
      if (cassandraProcessFound) {
        return true;
      } else {
        throw new RuntimeException("there is no grakn-cassandra process with PID " + pid);
      }
    } else {
      return false;
    }
  }

  public void startCassandra() throws IOException {
    osCalls.exec(new String[] { "sh", "-c", "bin/grakn-cassandra.sh start" });
  }

  public int stopCassandra() throws IOException, InterruptedException {
    Process kill = osCalls.exec(new String[]{ "sh", "-c", "bin/grakn-cassandra.sh stop" });
    return kill.waitFor();
  }

  private void waitForCassandraStarted() throws MalformedPidFileException, IOException, InterruptedException {
    final int MAX_CHECK_ATTEMPT = 3;
    int attempt = 0;
    while (attempt < MAX_CHECK_ATTEMPT) {
      if (isCassandraRunning()) {
        LOG.info("grakn-cassandra has been started successfully!");
        return ;
      } else {
        LOG.info("grakn-cassandra has not yet started. will re-attempt the check...");
        attempt++;
        Thread.sleep(WAIT_BETWEEN_ATTEMPT_MS);
      }
    }
    LOG.info("unable to start grakn-cassandra!");
    throw new GraknComponentSupervisionException("unable to start grakn-cassandra!");
  }

  private void waitForCassandraStopped() throws MalformedPidFileException, IOException, InterruptedException {
    final int MAX_CHECK_ATTEMPT = 3;
    int attempt = 0;
    while (attempt < MAX_CHECK_ATTEMPT) {
      if (!isCassandraRunning()) {
        LOG.info("grakn-cassandra has been stopped successfully!");
        return ;
      } else {
        LOG.info("grakn-cassandra has not been stopped. will re-attempt the check...");
        attempt++;
        Thread.sleep(WAIT_BETWEEN_ATTEMPT_MS);
      }
    }
    LOG.info("unable to stop grakn-cassandra!");
    throw new GraknComponentSupervisionException("unable to stop grakn-cassandra!");
  }
}