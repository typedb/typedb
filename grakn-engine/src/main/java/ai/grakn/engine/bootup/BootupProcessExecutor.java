/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.bootup;

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
            ProcessResult result = new ProcessExecutor().readOutput(true).redirectError(stderr).directory(workingDirectory).command(command).execute();
            return BootupProcessResult.create(result.outputUTF8(), stderr.toString(StandardCharsets.UTF_8.name()), result.getExitValue());
        }
        catch (IOException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public Optional<String> getPidFromFile(Path fileName) {
        String pid=null;
        if (fileName.toFile().exists()) {
            try {
                pid = new String(Files.readAllBytes(fileName),StandardCharsets.UTF_8).trim();
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

    public int retrievePid(Path pidFile) {
        if(!pidFile.toFile().exists()) {
            return -1;
        }
        try {
            String pid = new String(Files.readAllBytes(pidFile), StandardCharsets.UTF_8);
            pid = pid.trim();
            return Integer.parseInt(pid);
        } catch (NumberFormatException | IOException e) {
            return -1;
        }
    }

    public void waitUntilStopped(Path pidFile, int pid) {
        BootupProcessResult bootupProcessResult;
        do {
            System.out.print(".");
            System.out.flush();

            bootupProcessResult = kill(pid,"0"); // kill -0 <pid> does not kill the process. it simply checks if the process is still running.

            try {
                Thread.sleep(WAIT_INTERVAL_SECOND * 1000);
            } catch (InterruptedException e) {
                // DO NOTHING
            }
        } while (bootupProcessResult.success());
        System.out.println("SUCCESS");
        FileUtils.deleteQuietly(pidFile.toFile());
    }

    public boolean isProcessRunning(Path pidFile) {
        boolean isRunning = false;
        String processPid;
        if (pidFile.toFile().exists()) {
            try {
                processPid = new String(Files.readAllBytes(pidFile),StandardCharsets.UTF_8);
                if(processPid.trim().isEmpty()) {
                    return false;
                }
                BootupProcessResult command =
                        executeAndWait(Arrays.asList(SH, "-c", "ps -p "+processPid.trim()+" | grep -v CMD | wc -l"), null);
                return Integer.parseInt(command.stdout().trim())>0;
            } catch (NumberFormatException | IOException e) {
                return false;
            }
        }
        return isRunning;
    }

    public void stopProcessIfRunning(Path pidFile, String programName) {
        System.out.print("Stopping "+programName+"...");
        System.out.flush();
        boolean programIsRunning = isProcessRunning(pidFile);
        if(!programIsRunning) {
            System.out.println("NOT RUNNING");
        } else {
            stopProcess(pidFile);
        }

    }

    public void processStatus(Path storagePid, String name) {
        if (isProcessRunning(storagePid)) {
            System.out.println(name+": RUNNING");
        } else {
            System.out.println(name+": NOT RUNNING");
        }
    }

    private void stopProcess(Path pidFile) {
        int pid = retrievePid(pidFile);
        if (pid < 0 ) return;
        kill(pid);
        waitUntilStopped(pidFile, pid);
    }

    private void kill(int pid) {
        executeAndWait(Arrays.asList(SH, "-c", "kill " + pid), null);
    }

    private BootupProcessResult kill(int pid, String signal) {
        return executeAndWait(Arrays.asList(SH, "-c", "kill -" + signal + " " + pid), null);
    }
}
