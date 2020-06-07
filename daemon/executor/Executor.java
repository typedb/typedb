/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.daemon.executor;

import org.apache.commons.io.FileUtils;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/**
 * This class is responsible for spawning process.
 */
public class Executor {

    public static class Result {

        private final String stdout;
        private final String stderr;
        private final int exitCode;

        public Result(String stdout, String stderr, int exitCode) {
            this.stdout = stdout;
            this.stderr = stderr;
            this.exitCode = exitCode;
        }

        public boolean success() {
            return exitCode() == 0;
        }

        public String stdout() { return stdout; }

        public String stderr() { return stderr; }

        public int exitCode() { return exitCode; }
    }

    static final long WAIT_INTERVAL_SECOND = 2;
    private static final String SH = "/bin/sh";


    public CompletableFuture<Result> executeAsync(List<String> command, File workingDirectory) {
        return CompletableFuture.supplyAsync(() -> executeAndWait(command, workingDirectory));
    }

    public Result executeAndWait(List<String> command, File workingDirectory) {
        try {
            ByteArrayOutputStream stderr = new ByteArrayOutputStream();

            ProcessResult result = new ProcessExecutor()
                    .readOutput(true).redirectError(stderr).directory(workingDirectory)
                    .command(command)
                    .execute();

            return new Result(
                    result.outputUTF8(),
                    stderr.toString(StandardCharsets.UTF_8.name()),
                    result.getExitValue()
            );
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public String retrievePid(Path pidFile) {
        if (!pidFile.toFile().exists()) {
            return null;
        }
        try {
            String pid = new String(Files.readAllBytes(pidFile), StandardCharsets.UTF_8);
            return pid.trim();
        } catch (NumberFormatException | IOException e) {
            return null;
        }
    }

    public void waitUntilStopped(Path pidFile) {
        while (isProcessRunning(pidFile)) {
            System.out.print(".");
            System.out.flush();
            try {
                Thread.sleep(WAIT_INTERVAL_SECOND * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        System.out.println("SUCCESS");
        FileUtils.deleteQuietly(pidFile.toFile());
    }

    public boolean isProcessRunning(Path pidFile) {
        String processPid;
        if (pidFile.toFile().exists()) {
            try {
                processPid = new String(Files.readAllBytes(pidFile), StandardCharsets.UTF_8);
                if (processPid.trim().isEmpty()) {
                    return false;
                }
                Result command = executeAndWait(checkPIDRunningCommand(processPid), null);

                if (command.exitCode() != 0) {
                    System.out.println(command.stderr());
                }
                return command.exitCode() == 0;
            } catch (NumberFormatException | IOException e) {
                return false;
            }
        }
        return false;
    }


    /**
     * Method used to check whether the pid contained in the pid file actually corresponds
     * to a Grakn(Storage) process.
     *
     * @param pidFile path to pid file
     * @param className name of Class associated to the given pid (e.g. "grakn.core.server.Grakn")
     * @return true if PID is associated to the a Grakn process, false otherwise.
     */
    public boolean isAGraknProcess(Path pidFile, String className) {
        String processPid;
        if (pidFile.toFile().exists()) {
            try {
                processPid = new String(Files.readAllBytes(pidFile), StandardCharsets.UTF_8);
                if (processPid.trim().isEmpty()) {
                    return false;
                }
                Result command = executeAndWait(getGraknPIDArgsCommand(processPid), null);

                if (command.exitCode() != 0) {
                    return false;
                }
                return command.stdout().contains(className);
            } catch (NumberFormatException | IOException e) {
                return false;
            }
        }
        return false;
    }

    private List<String> checkPIDRunningCommand(String pid) {
        if (isWindows()) {
            return Arrays.asList("cmd", "/c", "tasklist /fi \"PID eq " + pid.trim() + "\" | findstr \"" + pid.trim() + "\"");
        } else {
            return Arrays.asList(SH, "-c", "ps -p " + pid.trim());
        }
    }

    private List<String> getGraknPIDArgsCommand(String pid) {
        if (isWindows()) {
            return Arrays.asList("cmd", "/c", "wmic process where processId='" + pid.trim() + "' get Commandline | findstr Grakn");
        } else {
            return Arrays.asList(SH, "-c", "ps -p " + pid.trim() + " -o command | awk '{print $NF}' | grep Grakn");
        }
    }

    public void stopProcessIfRunning(Path pidFile, String programName) {
        System.out.print("Stopping " + programName + "...");
        System.out.flush();
        boolean programIsRunning = isProcessRunning(pidFile);
        if (!programIsRunning) {
            System.out.println("NOT RUNNING");
        } else {
            stopProcess(pidFile);
        }

    }

    public void processStatus(Path pidFile, String name, String className) {
        if (isProcessRunning(pidFile) && isAGraknProcess(pidFile, className)) {
            System.out.println(name + ": RUNNING");
        } else {
            System.out.println(name + ": NOT RUNNING");
        }
    }

    private void stopProcess(Path pidFile) {
        String pid = retrievePid(pidFile);
        if (pid == null) return;
        kill(pid);
        waitUntilStopped(pidFile);
    }

    private List<String> killProcessCommand(String pid) {
        if (isWindows()) {
            return Arrays.asList("cmd", "/c", "taskkill /F /PID " + pid.trim());
        } else {
            return Arrays.asList(SH, "-c", "kill " + pid.trim());
        }
    }

    private void kill(String pid) {
        executeAndWait(killProcessCommand(pid), null);
    }

    private boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().contains("win");
    }
}
