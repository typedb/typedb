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

package ai.grakn.bootup;

import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

/**
 * This class is responsible for spawning process.
 *
 * @author Michele Orsi
 */
public class BootupProcessExecutor {

    public final long WAIT_INTERVAL_SECOND = 2;
    public final String SH = "/bin/sh";

    public OutputCommand executeAndWait(List<String> command, File workingDirectory) {
        try {
            ProcessResult result = new ProcessExecutor().readOutput(true).directory(workingDirectory).command(command).execute();
            return new OutputCommand(result.outputUTF8(), result.getExitValue());
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
                Arrays.asList(SH, "-c", "ps -ef | grep " + processName + " | grep -v grep | awk '{print $2}' "), null).output;
    }

    private void kill(int pid) {
        executeAndWait(Arrays.asList(SH, "-c", "kill " + pid), null);
    }

    private OutputCommand kill(int pid, String signal) {
        return executeAndWait(Arrays.asList(SH, "-c", "kill -" + signal + " " + pid), null);
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
        OutputCommand outputCommand;
        do {
            System.out.print(".");
            System.out.flush();

            outputCommand = kill(pid,"0");

            try {
                Thread.sleep(WAIT_INTERVAL_SECOND * 1000);
            } catch (InterruptedException e) {
                // DO NOTHING
            }
        } while (outputCommand.succes());
        System.out.println("SUCCESS");
        File file = pidFile.toFile();
        if(file.exists()) {
            try {
                Files.delete(pidFile);
            } catch (IOException e) {
                // DO NOTHING
            }
        }
    }

    public String selectCommand(String osx, String linux) {
        OutputCommand operatingSystem = executeAndWait(Arrays.asList(SH, "-c", "uname"), null);
        return operatingSystem.output.trim().equals("Darwin") ? osx : linux;
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
                OutputCommand command =
                        executeAndWait(Arrays.asList(SH, "-c", "ps -p "+processPid.trim()+" | grep -v CMD | wc -l"), null);
                return Integer.parseInt(command.output.trim())>0;
            } catch (NumberFormatException | IOException e) {
                return false;
            }
        }
        return isRunning;
    }

    public void stopProgram(Path pidFile, String programName) {
        System.out.print("Stopping "+programName+"...");
        System.out.flush();
        boolean programIsRunning = isProcessRunning(pidFile);
        if(!programIsRunning) {
            System.out.println("NOT RUNNING");
        } else {
            stopProcess(pidFile);
        }

    }

    void stopProcess(Path pidFile) {
        int pid = retrievePid(pidFile);
        if (pid <0 ) return;
        kill(pid);
        waitUntilStopped(pidFile, pid);
    }

    public void processStatus(Path storagePid, String name) {
        if (isProcessRunning(storagePid)) {
            System.out.println(name+": RUNNING");
        } else {
            System.out.println(name+": NOT RUNNING");
        }
    }
}
