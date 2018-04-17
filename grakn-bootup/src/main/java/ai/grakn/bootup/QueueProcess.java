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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;

/**
 *
 * @author Michele Orsi
 */
public class QueueProcess extends AbstractProcessHandler {
    private static final String QUEUE_PROCESS_NAME = "redis-server";
    private static final String CONFIG_LOCATION = "/services/redis/redis.conf";
    private static final Path QUEUE_PID = Paths.get(File.separator,"tmp","grakn-queue.pid");
    private static final long QUEUE_STARTUP_TIMEOUT_S = 10;

    private static final String COMPONENT_NAME = "Queue";

    private final Path homePath;

    public QueueProcess(Path homePath) {
        this.homePath = homePath;
    }

    public void start() {
        boolean queueRunning = processIsRunning(QUEUE_PID);
        if(queueRunning) {
            System.out.println(COMPONENT_NAME +" is already running");
        } else {
            queueStartProcess();
        }
    }
    private void queueStartProcess() {
        System.out.print("Starting "+ COMPONENT_NAME +"...");
        System.out.flush();
        String queueBin = selectCommand("redis-server-osx","redis-server-linux");

        // run queue
        // queue needs to be ran with $GRAKN_HOME as the working directory
        // otherwise it won't be able to find its data directory located at $GRAKN_HOME/db/redis
        executeAndWait(new String[]{
                SH,
                "-c",
                homePath +"/services/redis/"+queueBin+" "+ homePath + CONFIG_LOCATION
        },null,homePath.toFile());

        LocalDateTime init = LocalDateTime.now();
        LocalDateTime timeout = init.plusSeconds(QUEUE_STARTUP_TIMEOUT_S);

        while(LocalDateTime.now().isBefore(timeout)) {
            System.out.print(".");
            System.out.flush();

            if(processIsRunning(QUEUE_PID)) {
                System.out.println("SUCCESS");
                return;
            }
            try {
                Thread.sleep(WAIT_INTERVAL_S * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        System.out.println("FAILED!");
        System.out.println("Unable to start "+ COMPONENT_NAME);
        throw new ProcessNotStartedException();
    }

    public void stop() {
        System.out.print("Stopping "+ COMPONENT_NAME +"...");
        System.out.flush();
        boolean queueIsRunning = processIsRunning(QUEUE_PID);
        if(!queueIsRunning) {
            System.out.println("NOT RUNNING");
        } else {
            queueStopProcess();
        }
    }

    private void queueStopProcess() {
        int pid = retrievePid(QUEUE_PID);
        if (pid <0 ) return;

        String host = getHostFromConfig();
        String queueBin = selectCommand("redis-cli-osx", "redis-cli-linux");
        executeAndWait(new String[]{
                SH,
                "-c",
                homePath + "/services/redis/" + queueBin + " -h " + host + " shutdown"
        }, null, null);

        waitUntilStopped(QUEUE_PID,pid);
    }

    private String getHostFromConfig() {
        Path fileLocation = Paths.get(homePath.toString(), CONFIG_LOCATION);
        return GraknConfig.read(fileLocation.toFile()).getProperty(GraknConfigKey.REDIS_BIND);
    }

    public void status() {
        processStatus(QUEUE_PID, COMPONENT_NAME);
    }

    public void statusVerbose() {
        System.out.println(COMPONENT_NAME +" pid = '"+ getPidFromFile(QUEUE_PID).orElse("")+"' (from "+QUEUE_PID+"), '"+ getPidFromPsOf(QUEUE_PROCESS_NAME) +"' (from ps -ef)");
    }

    public void clean() {
        System.out.print("Cleaning "+ COMPONENT_NAME +"...");
        System.out.flush();
        start();
        String queueBin = selectCommand("redis-cli-osx", "redis-cli-linux");

        executeAndWait(new String[]{
                SH,
                "-c",
                homePath.resolve(Paths.get("services", "redis", queueBin))+" flushall"
        },null,null);
        stop();
        System.out.println("SUCCESS");
    }

    public boolean isRunning() {
        return processIsRunning(QUEUE_PID);
    }
}
