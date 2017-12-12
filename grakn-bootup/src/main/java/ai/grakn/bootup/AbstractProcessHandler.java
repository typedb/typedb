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

package ai.grakn.bootup;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 *
 * @author Michele Orsi
 */
abstract class AbstractProcessHandler {

    final static long WAIT_INTERVAL_S=2;

    OutputCommand executeAndWait(String[] cmdarray, String[] envp, File dir) {

        StringBuilder outputS = new StringBuilder();
        int exitValue = 1;

        Process p;
        BufferedReader reader = null;
        try {
            p = Runtime.getRuntime().exec(cmdarray, envp, dir);
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
        return new OutputCommand(outputS.toString().trim(), exitValue);
    }

    Optional<String> getPidFromFile(Path fileName) {
        String pid=null;
        if (Files.exists(fileName)) {
            try {
                pid = new String(Files.readAllBytes(fileName),StandardCharsets.UTF_8).trim();
            } catch (IOException e) {
                // DO NOTHING
            }
        }
        return Optional.ofNullable(pid);
    }

    String getPidFromPsOf(String processName) {
        return executeAndWait(new String[]{
                    "/bin/sh",
                    "-c",
                    "ps -ef | grep " + processName + " | grep -v grep | awk '{print $2}' "
            }, null, null).output;
    }

    private void kill(int pid) {
        executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                "kill " + pid
        }, null, null);
    }

    private OutputCommand kill(int pid, String signal) {
        return executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                "kill -"+signal+" " + pid
        }, null, null);
    }

    int retrievePid(Path pidFile) {
        if(!Files.exists(pidFile)) {
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

    void waitUntilStopped(Path pidFile, int pid) {
        OutputCommand outputCommand;
        do {
            System.out.print(".");
            System.out.flush();

            outputCommand = kill(pid,"0");

            try {
                Thread.sleep(WAIT_INTERVAL_S * 1000);
            } catch (InterruptedException e) {
                // DO NOTHING
            }
        } while (outputCommand.succes());
        System.out.println("SUCCESS");
        try {
            if(Files.exists(pidFile)) {
                Files.delete(pidFile);
            }
        } catch (IOException e) {
            // DO NOTHING
        }
    }

    String selectCommand(String osx, String linux) {
        OutputCommand operatingSystem = executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                "uname"
        },null,null);
        return operatingSystem.output.trim().equals("Darwin") ? osx : linux;
    }

    boolean processIsRunning(Path pidFile) {
        boolean isRunning = false;
        String processPid;
        if (Files.exists(pidFile)) {
            try {
                processPid = new String(Files.readAllBytes(pidFile),StandardCharsets.UTF_8);
                if(processPid.trim().isEmpty()) {
                    return false;
                }
                OutputCommand command = executeAndWait(new String[]{
                        "/bin/sh",
                        "-c",
                        "ps -p "+processPid.trim()+" | grep -v CMD | wc -l"
                },null,null);
                return Integer.parseInt(command.output.trim())>0;
            } catch (NumberFormatException | IOException e) {
                return false;
            }
        }
        return isRunning;
    }

    void stopProgram(Path pidFile, String programName) {
        System.out.print("Stopping "+programName+"...");
        System.out.flush();
        boolean programIsRunning = processIsRunning(pidFile);
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

    void processStatus(Path storagePid, String name) {
        if (processIsRunning(storagePid)) {
            System.out.println(name+": RUNNING");
        } else {
            System.out.println(name+": NOT RUNNING");
        }
    }
}
