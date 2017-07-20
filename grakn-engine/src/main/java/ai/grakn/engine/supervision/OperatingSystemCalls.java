package ai.grakn.engine.supervision;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class OperatingSystemCalls {
  public boolean fileExists(String path) {
    return Files.exists(Paths.get(path));
  }

  public int psP(int pid) throws IOException, InterruptedException {
    Process ps = Runtime.getRuntime().exec(new String[]{"sh", "-c", " ps -p " + pid});

    return ps.waitFor();
  }

  public int catPidFile(String file) throws MalformedPidFileException, IOException {
    Process catProcess = Runtime.getRuntime().exec(new String[]{"sh", "-c", "cat " + file});

    try (BufferedReader catStdout =
             new BufferedReader(new InputStreamReader(catProcess.getInputStream(), StandardCharsets.UTF_8))) {
      List<String> lines = catStdout.lines().collect(Collectors.toList());
      if (lines.size() == 1) {
        return Integer.parseInt(lines.get(0));
      } else {
        throw new MalformedPidFileException("a pid file should only have one line, however this one has " + lines.size() + " lines");
      }
    }
  }

  public Process exec(String[] args) throws IOException {
    return Runtime.getRuntime().exec(args);
  }
}
