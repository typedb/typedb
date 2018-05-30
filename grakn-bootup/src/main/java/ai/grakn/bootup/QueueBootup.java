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
import java.util.concurrent.TimeoutException;

/**
 * A class responsible for managing the Queue process,
 * including starting, stopping, status checks, and cleaning the Queue data
 *
 * @author Ganeshwara Herawan Hananda
 * @author Michele Orsi
 */
public class QueueBootup extends AbstractProcessHandler {
    private static final String DISPLAY_NAME = "Queue";
    private static final String QUEUE_PROCESS_NAME = "redis-server";
    private static final long QUEUE_STARTUP_TIMEOUT_SECOND = 10;
    private static final Path QUEUE_CONFIG_PATH = Paths.get("services", "redis", "redis.conf");
    private static final Path QUEUE_PIDFILE = Paths.get(File.separator,"tmp", "grakn-queue.pid");
    private final Path QUEUE_SERVER_BIN = Paths.get("services", "redis", selectCommand("redis-server-osx", "redis-server-linux"));
    private final Path QUEUE_CLI_BIN = Paths.get("services", "redis", selectCommand("redis-cli-osx", "redis-cli-linux"));

    private final Path graknHome;

    public QueueBootup(Path graknHome) {
        this.graknHome = graknHome;
    }

    public void startIfNotRunning() {
        boolean queueRunning = isProcessRunning(QUEUE_PIDFILE);
        if(queueRunning) {
            System.out.println(DISPLAY_NAME +" is already running");
        } else {
            start();
        }
    }
    private void start() {
        System.out.print("Starting "+ DISPLAY_NAME +"...");
        System.out.flush();

        startRedisServer();

        LocalDateTime init = LocalDateTime.now();
        LocalDateTime timeout = init.plusSeconds(QUEUE_STARTUP_TIMEOUT_SECOND);

        while(LocalDateTime.now().isBefore(timeout)) {
            System.out.print(".");
            System.out.flush();

            if(isProcessRunning(QUEUE_PIDFILE)) {
                System.out.println("SUCCESS");
                return;
            }
            try {
                Thread.sleep(WAIT_INTERVAL_SECOND * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        System.out.println("FAILED!");
        System.out.println("Unable to start "+ DISPLAY_NAME);
        throw new ProcessNotStartedException();
    }

    public void stop() {
        System.out.print("Stopping "+ DISPLAY_NAME +"...");
        System.out.flush();
        boolean queueIsRunning = isProcessRunning(QUEUE_PIDFILE);
        if(!queueIsRunning) {
            System.out.println("NOT RUNNING");
        } else {
            queueStopProcess();
        }
    }

    private void queueStopProcess() {
        int pid = retrievePid(QUEUE_PIDFILE);
        if (pid <0 ) return;

        String host = getHostFromConfig();
        redisCliCheckIfRedisServerIsStarted(host);
        waitUntilStopped(QUEUE_PIDFILE,pid);
    }

    private String getHostFromConfig() {
        Path fileLocation = graknHome.resolve(QUEUE_CONFIG_PATH);
        return GraknConfig.read(fileLocation.toFile()).getProperty(GraknConfigKey.REDIS_BIND);
    }

    public void status() {
        processStatus(QUEUE_PIDFILE, DISPLAY_NAME);
    }

    public void statusVerbose() {
        System.out.println(DISPLAY_NAME +" pid = '" + getPidFromFile(QUEUE_PIDFILE).orElse("") +
                "' (from "+ QUEUE_PIDFILE +"), '" + getPidFromPsOf(QUEUE_PROCESS_NAME) +"' (from ps -ef)");
    }

    public void clean() {
        System.out.print("Cleaning "+ DISPLAY_NAME +"...");
        System.out.flush();
        startIfNotRunning();
        redisCliClean();
        stop();
        System.out.println("SUCCESS");
    }

    public boolean isRunning() {
        return isProcessRunning(QUEUE_PIDFILE);
    }

    /**
     * Executes the following command:
     * ./services/redis/redis-server-osx ./services/redis/redis.conf
     */
    private void startRedisServer() {
        try {
            new ProcessExecutor()
                    .readOutput(true)
                    .directory(graknHome.toFile())
                    .command(QUEUE_SERVER_BIN.toString(), QUEUE_CONFIG_PATH.toString())
                    .execute();
        }
        catch (IOException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Executes the following command:
     * ./services/redis/redis-cli-osx -h <host> shutdown
     */
    private void redisCliCheckIfRedisServerIsStarted(String host) {
        try {
            new ProcessExecutor()
                    .readOutput(true)
                    .directory(graknHome.toFile())
                    .command(QUEUE_CLI_BIN.toString(), "-h", host, "shutdown")
                    .execute();
        }
        catch (IOException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Executes the following command:
     * ./services/redis/redis-cli-osx flushall
     */
    private void redisCliClean() {
        try {
            new ProcessExecutor()
                    .readOutput(true)
                    .directory(graknHome.toFile())
                    .command(QUEUE_CLI_BIN.toString(), "flushall")
                    .execute();
        }
        catch (IOException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
