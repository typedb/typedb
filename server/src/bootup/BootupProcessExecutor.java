/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package ai.grakn.core.server.bootup;

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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/**
 * This class is responsible for spawning process.
 *
 * @author Michele Orsi
 */
public class BootupProcessExecutor {

    public static final long WAIT_INTERVAL_SECOND = 2;
    public static final String SH = "/bin/sh";

    public CompletableFuture<BootupProcessResult> executeAsync(List<String> command, File workingDirectory) {
        return CompletableFuture.supplyAsync(() -> executeAndWait(command, workingDirectory));
    }

    public BootupProcessResult executeAndWait(List<String> command, File workingDirectory) {
        try {
            ByteArrayOutputStream stderr = new ByteArrayOutputStream();
            ProcessResult result = new ProcessExecutor()
                    .readOutput(true)
                    .redirectError(stderr)
                    .directory(workingDirectory).command(command).execute();
            return BootupProcessResult.create(result.outputUTF8(), stderr.toString(StandardCharsets.UTF_8.name()), result.getExitValue());
        } catch (IOException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public Optional<String> getPidFromFile(Path fileName) {
        String pid = null;
        if (fileName.toFile().exists()) {
            try {
                pid = new String(Files.readAllBytes(fileName), StandardCharsets.UTF_8).trim();
            } catch (IOException e) {
                // DO NOTHING
            }
        }
        return Optional.ofNullable(pid);
    }

    public String getPidFromPsOf(String processName) {
        return executeAndWait(
                Arrays.asList(SH, "-c", "ps -ef | grep " + processName + " | grep -v grep | awk '{print $2}' "), null).stdout();
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
                // DO NOTHING
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
                BootupProcessResult command =
                        executeAndWait(checkPIDRunningCommand(processPid), null);

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

    private List<String> checkPIDRunningCommand(String pid) {
        if (isWindows()) {
            return Arrays.asList("cmd", "/c", "tasklist /fi \"PID eq " + pid.trim() + "\" | findstr \"" + pid.trim() + "\"");
        } else {
            return Arrays.asList(SH, "-c", "ps -p " + pid.trim());
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

    public void processStatus(Path storagePid, String name) {
        if (isProcessRunning(storagePid)) {
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

    private List<String> killProcessCommand(String pid){
        if (isWindows()) {
            return Arrays.asList("cmd", "/c", "taskkill /F /PID "+ pid.trim());
        } else {
            return Arrays.asList(SH, "-c", "kill " + pid.trim());
        }
    }

    private void kill(String pid) { executeAndWait(killProcessCommand(pid), null); }

    private boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().contains("win");
    }
}
