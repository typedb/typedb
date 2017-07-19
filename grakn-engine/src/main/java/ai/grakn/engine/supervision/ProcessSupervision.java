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
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Responsible for supervising cassandra and redis processes.
 *
 * @author Ganeshwara Herawan Hananda
 */

public class ProcessSupervision {
  private static final Logger LOG = Logger.getLogger(ProcessSupervision.class.getName());
  private static final String CASSANDRA_FULL_PATH = "bin/cassandra"; // TODO: this exe shouldn't even be exposed anymore
  private static final String CASSANDRA_PID_FILE = "/tmp/grakn-cassandra.pid";

  public void startCassandraIfNotExists() {
    LOG.info("checking if there exists a running grakn-cassandra process...");
    if (!isCassandraRunning()) {
      LOG.info("grakn-cassandra isn't yet running. attempting to start...");
      startCassandra();
      waitForCassandraStarted();

    } else {
      LOG.info("found an existing grakn-cassandra process.");
    }
  }

  public void stopCassandraIfRunning() {
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

  public boolean isCassandraRunning() {
    if (fileExists(CASSANDRA_PID_FILE)) {
      int pid = catPidFile(CASSANDRA_PID_FILE);
      boolean cassandraProcessFound = psP(pid) == 0;
      if (cassandraProcessFound) {
        return true;
      }
      else {
        throw new RuntimeException("there is no grakn-cassandra process with PID " + pid);
      }
    }
    else {
      return false;
    }
  }

  public void startCassandra() {
    try {
      Runtime.getRuntime().exec(new String[] { "sh", "-c", "bin/grakn-cassandra.sh start" });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public int stopCassandra() {
    try {
      Process kill = Runtime.getRuntime().exec(new String[]{ "sh", "-c", "bin/grakn-cassandra.sh stop" });
      return kill.waitFor();
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean fileExists(String path) {
    return Files.exists(Paths.get(path));
  }

  public int psP(int pid) {
    try {
      Process ps = Runtime.getRuntime().exec(new String[]{"sh", "-c", " ps -p " + pid});

      return ps.waitFor();
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public int catPidFile(String file) {
    try {
      Process catProcess = Runtime.getRuntime().exec(new String[]{"sh", "-c", "cat " + file});

      try (BufferedReader catStdout =
               new BufferedReader(new InputStreamReader(catProcess.getInputStream(), StandardCharsets.UTF_8))) {
        List<String> lines = catStdout.lines().collect(Collectors.toList());
        if (lines.size() == 1) {
          return Integer.parseInt(lines.get(0));
        } else {
          throw new RuntimeException("a pid file should only have one line, however this one has " + lines.size() + " lines");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void waitForCassandraStarted() {
    final int MAX_CHECK_ATTEMPT = 3;
    int attempt = 0;
    while (attempt < MAX_CHECK_ATTEMPT) {
      if (isCassandraRunning()) {
        LOG.info("grakn-cassandra has been started successfully!");
        return ;
      } else {
        LOG.info("grakn-cassandra has not yet started. will re-attempt the check...");
        attempt++;
        threadSleep(2000);
      }
    }
    LOG.info("unable to start grakn-cassandra!");
    throw new RuntimeException("unable to start grakn-cassandra!");
  }

  private void waitForCassandraStopped() {
    final int MAX_CHECK_ATTEMPT = 3;
    int attempt = 0;
    while (attempt < MAX_CHECK_ATTEMPT) {
      if (!isCassandraRunning()) {
        LOG.info("grakn-cassandra has been stopped successfully!");
        return ;
      } else {
        LOG.info("grakn-cassandra has not been stopped. will re-attempt the check...");
        attempt++;
        threadSleep(2000);
      }
    }
    LOG.info("unable to stop grakn-cassandra!");
    throw new RuntimeException("unable to stop grakn-cassandra!");
  }

  private void threadSleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}