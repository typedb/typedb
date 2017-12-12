package ai.grakn.bootup.graknengine.grakn_pid;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class UnixGraknPidRetriever implements GraknPidRetriever {
    public long getPid() {
        StringBuilder outputS = new StringBuilder();
        int exitValue = 1;

        Process p;
        BufferedReader reader = null;
        try {
            p = Runtime.getRuntime().exec(new String[] { "/bin/sh", "-c", "ps -ef | ps -ef | grep \"ai.grakn.engine.Grakn\" | grep -v grep | awk '{print $2}'" }, null, null);
            p.waitFor();
            exitValue = p.exitValue();
            reader =
                    new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));

            String line;
            while ((line = reader.readLine()) != null) {
                outputS.append(line).append("\n");
            }

        } catch (InterruptedException | IOException e) {
            // DO NOTHING
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    // DO NOTHING
                }
            }
        }

        String pidString = outputS.toString().trim();
        try {
            long pid = Long.parseLong(pidString);
            return pid;
        } catch (NumberFormatException e) {
            throw new RuntimeException("Couldn't get PID of Grakn. Received '" + pidString);
        }
    }
}
