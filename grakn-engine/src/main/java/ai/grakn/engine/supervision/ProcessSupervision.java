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
  private static final String CASSANDRA_FULL_PATH = "Users/lolski/Downloads/grakn-dist-0.14.0/bin/cassandra";
  private static final String CASSANDRA_PID_FILE = "tmp/grakn-cassandra.pid";

  public static void startCassandraIfNotExists() {
    LOG.info("checking if there exists a running grakn-cassandra process...");
    if (!ProcessSupervision.isCassandraRunning()) {
      LOG.info("grakn-cassandra isn't yet running. attempting to start...");
      ProcessSupervision.startCassandra();

      // attempt a check several times to see if it's actually running
      int attempt = 0;
      while (attempt < 3) {
        if (isCassandraRunning()) {
          LOG.info("grakn-cassandra has been started successfully!");
          return ;
        } else {
          // it's not yet running. pause for a bit and re-check
          LOG.info("grakn-cassandra has not yet started. will re-attempt the check...");
          attempt++;
          threadSleep(1000);
        }
      }
      LOG.info("unable to start grakn-cassandra!");
      throw new RuntimeException("unable to start grakn-cassandra!");
    } else {
      LOG.info("found an existing grakn-cassandra process.");
    }
  }

  public static void stopCassandraIfRunning() {
    if (ProcessSupervision.isCassandraRunning()) {
      ProcessSupervision.stopCassandra();
    }
  }

  private static boolean isCassandraRunning() {
    try {
      return fileExists(CASSANDRA_PID_FILE) && psP(catPidFile(CASSANDRA_PID_FILE)) == 0;
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static void startCassandra() {
    try {
      Runtime.getRuntime().exec(new String[] { "sh", "-c", CASSANDRA_FULL_PATH + " -p " + CASSANDRA_PID_FILE });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static int stopCassandra() {
    try {
      Process kill = Runtime.getRuntime().exec(new String[]{ "sh", "-c", "kill " + catPidFile(CASSANDRA_PID_FILE) });
      return kill.waitFor();
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean fileExists(String path) {
    return Files.exists(Paths.get(path));
  }

  private static int psP(int pid) throws IOException, InterruptedException {
    Process ps = Runtime.getRuntime().exec(new String[] {"sh", "-c", " ps -p " + pid });
    return ps.waitFor();
  }

  private static int catPidFile(String file) throws IOException {
    Process catProcess = Runtime.getRuntime().exec(new String[] { "sh", "-c", "cat " + file });
    try (BufferedReader catStdout =
             new BufferedReader(new InputStreamReader(catProcess.getInputStream(), StandardCharsets.UTF_8))) {
      List<String> lines = catStdout.lines().collect(Collectors.toList());
      if (lines.size() == 1) {
        return Integer.parseInt(lines.get(0));
      }
      else {
        throw new RuntimeException("a pid file should only have one line, however this one has " + lines.size() + " lines");
      }
    }
  }

  private static void threadSleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}