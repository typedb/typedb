package ai.grakn.engine.supervision;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Timer;
import java.util.stream.Collectors;

/**
 * Responsible for supervising cassandra and redis processes.
 *
 * @author Ganeshwara Herawan Hananda
 */

public class ProcessSupervision {
  private static final String CASSANDRA_FULL_PATH = "/Users/lolski/Downloads/grakn-dist-0.14.0/bin/cassandra";
  private static final String CASSANDRA_PID_FILE = "/tmp/grakn-cassandra.pid";

  public static void startCassandraIfNotExists() {
    if (!ProcessSupervision.isCassandraRunning()) {
      ProcessSupervision.startCassandra();
      // TODO: attempt to check if cassandra is started successfully for a number of times every few seconds
    }
  }

  public static void stopCassandraIfRunning() {
    if (ProcessSupervision.isCassandraRunning()) {
      ProcessSupervision.stopCassandra();
    }
  }

  private static boolean isCassandraRunning() {
    try {
      return fileExists(CASSANDRA_PID_FILE) && psP(Integer.parseInt(cat(CASSANDRA_PID_FILE))) == 0;
    }
    catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static void startCassandra() {
    try {
      Runtime.getRuntime().exec(new String[] { "sh", "-c", CASSANDRA_FULL_PATH + " -p " + CASSANDRA_PID_FILE });
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static int stopCassandra() {
    try {
      Process kill = Runtime.getRuntime().exec(new String[]{ "sh", "-c", "kill " + cat(CASSANDRA_PID_FILE) });
      return kill.waitFor();
    }
    catch (IOException | InterruptedException e) {
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

  private static String cat(String file) throws IOException {
    Process cat = Runtime.getRuntime().exec(new String[] { "sh", "-c", "cat " + file });
    BufferedReader catStdout = new BufferedReader(new InputStreamReader(cat.getInputStream()));
    return catStdout.lines().collect(Collectors.joining("\n"));
  }
}