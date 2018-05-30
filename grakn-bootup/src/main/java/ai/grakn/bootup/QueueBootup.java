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

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.GraknConfig;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

/**
 * A class responsible for managing the Queue process,
 * including starting, stopping, status checks, and cleaning the Queue data
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

        QUEUE_SERVER_BIN = Paths.get("services", "redis", bootupProcessExecutor.selectCommand("redis-server-osx", "redis-server-linux"));
        QUEUE_CLI_BIN = Paths.get("services", "redis", bootupProcessExecutor.selectCommand("redis-cli-osx", "redis-cli-linux"));
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

        bootupProcessExecutor.executeAndWait(Arrays.asList(QUEUE_SERVER_BIN.toString(), QUEUE_CONFIG_PATH.toString()), graknHome.toFile());

        LocalDateTime timeout = LocalDateTime.now().plusSeconds(QUEUE_STARTUP_TIMEOUT_SECOND);

        while(LocalDateTime.now().isBefore(timeout)) {
            System.out.print(".");
            System.out.flush();

            if(bootupProcessExecutor.isProcessRunning(QUEUE_PIDFILE)) {
                System.out.println("SUCCESS");
                return;
            }
            try {
                Thread.sleep(bootupProcessExecutor.WAIT_INTERVAL_SECOND * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        System.out.println("FAILED!");
        System.out.println("Unable to start "+ DISPLAY_NAME);
        throw new ProcessNotStartedException();
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
}
