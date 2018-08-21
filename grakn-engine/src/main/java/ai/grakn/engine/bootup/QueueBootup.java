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

package ai.grakn.engine.bootup;

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.GraknConfig;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Arrays;

import static ai.grakn.engine.bootup.BootupProcessExecutor.SH;
import static ai.grakn.engine.bootup.BootupProcessExecutor.WAIT_INTERVAL_SECOND;

/**
 * A class responsible for managing the bootup-related process for the Queue component, including
 * starting and stopping, performing status checks, and cleaning the data.
 *
 * The PID file for the Storage component is managed internally by Cassandra and not by this class. This means that
 * you will not find any code which creates or deletes the PID file for the Storage component.
 *
 * @author Ganeshwara Herawan Hananda
 * @author Michele Orsi
 */
public class QueueBootup {
    private static final String DISPLAY_NAME = "Queue";
    private static final String QUEUE_PROCESS_NAME = "redis-server";
    private static final long QUEUE_STARTUP_TIMEOUT_SECOND = 10;
    private static final Path QUEUE_CONFIG_PATH = Paths.get("services", "redis", "redis.conf");
    private static final Path QUEUE_PIDFILE = Paths.get(File.separator,"tmp", "grakn-queue.pid");
    private final Path QUEUE_SERVER_BIN;
    private final Path QUEUE_CLI_BIN;

    private BootupProcessExecutor bootupProcessExecutor;
    private final Path graknHome;

    public QueueBootup(BootupProcessExecutor bootupProcessExecutor, Path graknHome) {
        this.graknHome = graknHome;
        this.bootupProcessExecutor = bootupProcessExecutor;

        QUEUE_SERVER_BIN = Paths.get("services", "redis", selectCommand("redis-server-osx", "redis-server-linux"));
        QUEUE_CLI_BIN = Paths.get("services", "redis", selectCommand("redis-cli-osx", "redis-cli-linux"));
    }

    public void startIfNotRunning() {
        boolean queueRunning = bootupProcessExecutor.isProcessRunning(QUEUE_PIDFILE);
        if(queueRunning) {
            System.out.println(DISPLAY_NAME +" is already running");
        } else {
            start();
        }
    }

    public void stop() {
        System.out.print("Stopping "+ DISPLAY_NAME +"...");
        System.out.flush();
        boolean queueIsRunning = bootupProcessExecutor.isProcessRunning(QUEUE_PIDFILE);
        if(!queueIsRunning) {
            System.out.println("NOT RUNNING");
        } else {
            queueStopProcess();
        }
    }

    public void status() {
        bootupProcessExecutor.processStatus(QUEUE_PIDFILE, DISPLAY_NAME);
    }

    public void statusVerbose() {
        System.out.println(DISPLAY_NAME +" pid = '" + bootupProcessExecutor.getPidFromFile(QUEUE_PIDFILE).orElse("") +
                "' (from "+ QUEUE_PIDFILE +"), '" + bootupProcessExecutor.getPidFromPsOf(QUEUE_PROCESS_NAME) +"' (from ps -ef)");
    }

    public void clean() {
        System.out.print("Cleaning "+ DISPLAY_NAME +"...");
        System.out.flush();
        startIfNotRunning();
        bootupProcessExecutor.executeAndWait(Arrays.asList(QUEUE_CLI_BIN.toString(), "flushall"), graknHome.toFile());
        stop();
        System.out.println("SUCCESS");
    }

    public boolean isRunning() {
        return bootupProcessExecutor.isProcessRunning(QUEUE_PIDFILE);
    }

    private void start() {
        System.out.print("Starting "+ DISPLAY_NAME +"...");
        System.out.flush();

        BootupProcessResult startQueue = bootupProcessExecutor.executeAndWait(Arrays.asList(QUEUE_SERVER_BIN.toString(), QUEUE_CONFIG_PATH.toString()), graknHome.toFile());

        LocalDateTime timeout = LocalDateTime.now().plusSeconds(QUEUE_STARTUP_TIMEOUT_SECOND);

        while(LocalDateTime.now().isBefore(timeout)) {
            System.out.print(".");
            System.out.flush();

            if(bootupProcessExecutor.isProcessRunning(QUEUE_PIDFILE)) {
                System.out.println("SUCCESS");
                return;
            }
            try {
                Thread.sleep(WAIT_INTERVAL_SECOND * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        String errorMessage = "Process exited with code " + startQueue.exitCode() + ": '" + startQueue.stderr() + "'";
        System.out.println("FAILED!");
        System.err.println("Unable to start " + DISPLAY_NAME + ". " + errorMessage);
        throw new BootupException();
    }

    private void queueStopProcess() {
        int pid = bootupProcessExecutor.retrievePid(QUEUE_PIDFILE);
        if (pid < 0 ) return;

        String host = getHostFromConfig();
        bootupProcessExecutor.executeAndWait(Arrays.asList(QUEUE_CLI_BIN.toString(), "-h", host, "shutdown"), graknHome.toFile());
        bootupProcessExecutor.waitUntilStopped(QUEUE_PIDFILE,pid); // TODO: this might be uneeded. verify and remove.
    }

    private String getHostFromConfig() {
        Path fileLocation = graknHome.resolve(QUEUE_CONFIG_PATH);
        return GraknConfig.read(fileLocation.toFile()).getProperty(GraknConfigKey.REDIS_BIND);
    }

    private String selectCommand(String osx, String linux) {
        BootupProcessResult operatingSystem = bootupProcessExecutor.executeAndWait(Arrays.asList(SH, "-c", "uname"), null);
        return operatingSystem.stdout().trim().equals("Darwin") ? osx : linux;
    }


}
