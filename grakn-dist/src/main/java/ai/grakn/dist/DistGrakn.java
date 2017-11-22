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

package ai.grakn.dist;

import ai.grakn.GraknConfigKey;
import ai.grakn.GraknSystemProperty;
import ai.grakn.engine.Grakn;
import ai.grakn.engine.GraknConfig;
import ai.grakn.graql.GraqlShell;
import ai.grakn.util.REST;
import ai.grakn.util.SimpleURI;

import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Scanner;

/**
 * 
 * @author Michele Orsi
 */
public class DistGrakn {

    private static final String GRAKN = "grakn";
    private static final String QUEUE = "queue";
    private static final String STORAGE = "storage";

    private static final Path GRAKN_PID = Paths.get(File.separator,"tmp","grakn.pid");
    private static final Path QUEUE_PID = Paths.get(File.separator,"tmp","grakn-queue.pid");
    private static final Path STORAGE_PID = Paths.get(File.separator,"tmp","grakn-storage.pid");

    private static final long STORAGE_STARTUP_TIMEOUT_S=60;
    private static final long QUEUE_STARTUP_TIMEOUT_S = 10;
    private static final long GRAKN_STARTUP_TIMEOUT_S = 120;

    private static final long WAIT_INTERVAL_S=2;

    private final GraknConfig graknConfig;
    private final Path configPath;
    private final Path homePath;

    private boolean storageIsStarted;
    private boolean queueIsStarted;
    private boolean graknIsStarted;
    private String classpath;

    private final ProcessHandler processHandler;

    /**
     * In order to run this method you should have 'grakn.dir' and 'grakn.conf' set
     *
     * @param args
     */
    public static void main(String[] args) {
        Path homeStatic;
        Path configStatic;
        DistGrakn application;
        try {
            homeStatic = Paths.get(GraknSystemProperty.CURRENT_DIRECTORY.value());
            configStatic = Paths.get(GraknSystemProperty.CONFIGURATION_FILE.value());
            application = new DistGrakn(homeStatic,configStatic, new ProcessHandler());

            String context = args.length > 0 ? args[0] : "";
            String action = args.length > 1 ? args[1] : "";
            String option = args.length > 2 ? args[2] : "";
            application.run(context,action,option);
        } catch (InvalidPathException ex) {
            System.out.println("Problem with bash script: cannot run Graql");
            return;
        } catch (RuntimeException ex) {
            System.out.println(ex.getMessage());
            return;
        }
    }

    public void run(String context, String action, String option) {
        classpath = processHandler.getClassPathFrom(homePath);

        switch (context) {
            case "server":
                server(action, option);
                break;
            case "version":
                version();
                break;
            default:
                help();
        }
    }

    public DistGrakn(Path homePath, Path configPath, ProcessHandler processHandler) {
        this.homePath = homePath;
        this.configPath = configPath;
        this.graknConfig = GraknConfig.read(configPath.toFile());
        this.processHandler = processHandler;
    }

    private void version() {
        GraqlShell.main(new String[]{"--v"});
    }

    private void help() {
        System.out.println("Usage: grakn COMMAND\n" +
                "\n" +
                "COMMAND:\n" +
                "server     Manage Grakn components\n" +
                "version    Print Grakn version\n" +
                "help       Print this message\n" +
                "\n" +
                "Tips:\n" +
                "- Start Grakn with 'grakn server start' (by default, the dashboard will be accessible at http://localhost:4567)\n" +
                "- You can then perform queries by opening a console with 'graql console'");
    }

    private void server(String action, String option) {
        switch (action) {
            case "start":
                graknServerStart(option);
                break;
            case "stop":
                graknServerStop(option);
                break;
            case "status":
                graknServerStatus(option);
                break;
            case "clean":
                graknServerClean();
                break;
            default:
                serverHelp();
        }
    }

    private void graknServerClean() {
        boolean storage = storageIsRunning();
        boolean queue = queueIsRunning();
        boolean grakn = graknIsRunning();
        if(storage || queue || grakn) {
            System.out.println("Grakn is still running! Please do a shutdown with 'grakn server stop' before performing a cleanup.");
        } else {
            System.out.print("Are you sure you want to delete all stored data and logs? [y/N] ");
            System.out.flush();
            String response = new Scanner(System.in, StandardCharsets.UTF_8.name()).next();
            if(!response.equals("y") && !response.equals("Y")) {
                System.out.println("Response '"+response+"' did not equal 'y' or 'Y'.  Canceling clean operation.");
                return;
            }
            System.out.print("Cleaning Storage...");
            System.out.flush();
            try {
                Files.delete(homePath.resolve(Paths.get("db","cassandra")));
                Files.createDirectories(homePath.resolve(Paths.get("db","cassandra","data")));
                Files.createDirectories(homePath.resolve(Paths.get("db","cassandra","commitlog")));
                Files.createDirectories(homePath.resolve(Paths.get("db","cassandra","saved_caches")));
                System.out.println("SUCCESS");
            } catch (IOException e) {
                System.out.println("FAILED!");
                System.out.println("Unable to clean Storage");
            }

            System.out.print("Cleaning Queue...");
            System.out.flush();
            queueStart();
            queueWipeAllData();
            stopQueue();
            System.out.println("SUCCESS");

            System.out.print("Cleaning Grakn...");
            System.out.flush();
            try {
                Files.delete(Paths.get(homePath +"logs"));
                Files.createDirectories(Paths.get(homePath +"logs"));
                System.out.println("SUCCESS");
            } catch (IOException e) {
                System.out.println("FAILED!");
                System.out.println("Unable to clean Grakn");
            }

        }
    }

    private void queueWipeAllData() {
        String queueBin = selectCommand("redis-cli-osx", "redis-cli-linux");

        processHandler.executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                homePath.resolve(Paths.get("services", "redis", queueBin))+" flushall"
        },null,null);
    }

    private String selectCommand(String osx, String linux) {
        OutputCommand operatingSystem = processHandler.executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                "uname"
        },null,null);
        return operatingSystem.output.trim().equals("Darwin") ? osx : linux;
    }

    private void graknServerStop(String arg) {
        switch (arg) {
            case GRAKN: stopGrakn();
                break;
            case QUEUE: stopQueue();
                break;
            case STORAGE: stopStorage();
                break;
            default: stopAll();
        }
    }

    private void stopAll() {
        stopGrakn();
        stopQueue();
        stopStorage();
    }

    public void stopStorage() {
        System.out.print("Stopping Storage...");
        System.out.flush();
        boolean storageIsRunning = storageIsRunning();
        if(!storageIsRunning) {
            System.out.println("NOT RUNNING");
        } else {
            stopProcess(STORAGE_PID);
        }
    }

    public void stopQueue() {
        System.out.print("Stopping Queue...");
        System.out.flush();
        boolean queueIsRunning = queueIsRunning();
        if(!queueIsRunning) {
            System.out.println("NOT RUNNING");
        } else {
            queueStopProcess();
        }
    }

    public void stopGrakn() {
        System.out.print("Stopping Grakn...");
        System.out.flush();
        boolean graknIsRunning = graknIsRunning();
        if(!graknIsRunning) {
            System.out.println("NOT RUNNING");
        } else {
            stopProcess(GRAKN_PID);
        }
    }

    private void graknServerStart(String arg) {
        switch (arg) {
            case GRAKN: startGrakn();
                break;
            case QUEUE:
                queueStart();
                break;
            case STORAGE: startStorage();
                break;
            default: startAll();
        }

    }

    private boolean checkIfRunningBy(Path pidFile) {
        boolean isRunning = false;
        String processPid;
        if (Files.exists(pidFile)) {
            try {
                processPid = new String(Files.readAllBytes(pidFile),StandardCharsets.UTF_8);
                if(processPid.trim().isEmpty()) {
                    return false;
                }
                OutputCommand command = processHandler.executeAndWait(new String[]{
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

    public boolean storageIsRunning() {
        return checkIfRunningBy(STORAGE_PID);
    }

    public boolean queueIsRunning() {
        return checkIfRunningBy(QUEUE_PID);
    }

    public boolean graknIsRunning() {
        return checkIfRunningBy(GRAKN_PID);
    }

    private void startStorage() {
        boolean storageIsRunning = storageIsRunning();
        if(storageIsRunning) {
            System.out.println("Storage is already running");
            storageIsStarted =true;
        } else {
            storageStartProcess();
        }
    }

    private void storageStartProcess() {
        System.out.print("Starting Storage...");
        System.out.flush();
        if(Files.exists(STORAGE_PID)) {
            try {
                Files.delete(STORAGE_PID);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        OutputCommand outputCommand = processHandler.executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                homePath.resolve(Paths.get("services","cassandra","cassandra")) + " -p " + STORAGE_PID
        }, null, null);
        LocalDateTime init = LocalDateTime.now();
        LocalDateTime timeout = init.plusSeconds(STORAGE_STARTUP_TIMEOUT_S);

        while(LocalDateTime.now().isBefore(timeout) && outputCommand.exitStatus<1) {
            System.out.print(".");
            System.out.flush();

            OutputCommand storageStatus = processHandler.executeAndWait(new String[]{
                    "/bin/sh",
                    "-c",
                    homePath + "/services/cassandra/nodetool statusthrift 2>/dev/null | tr -d '\n\r'"
            },null,null);
            if(storageStatus.output.trim().equals("running")) {
                System.out.println("SUCCESS");
                storageIsStarted =true;
                return;
            }
            try {
                Thread.sleep(WAIT_INTERVAL_S*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("FAILED!");
        System.out.println("Unable to start Storage");
    }

    private void queueStart() {
        boolean queueRunning = queueIsRunning();
        if(queueRunning) {
            System.out.println("Queue is already running");
            queueIsStarted =true;
        } else {
            queueStartProcess();
        }
    }

    private void queueStartProcess() {
        System.out.print("Starting Queue...");
        System.out.flush();
        String queueBin = selectCommand("redis-server-osx","redis-server-linux");

        // run queue
        // queue needs to be ran with $GRAKN_HOME as the working directory
        // otherwise it won't be able to find its data directory located at $GRAKN_HOME/db/redis
        processHandler.executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                homePath +"/services/redis/"+queueBin+" "+ homePath +"/services/redis/redis.conf"
        },null,homePath.toFile());

        LocalDateTime init = LocalDateTime.now();
        LocalDateTime timeout = init.plusSeconds(QUEUE_STARTUP_TIMEOUT_S);

        while(LocalDateTime.now().isBefore(timeout)) {
            System.out.print(".");
            System.out.flush();

            if(queueIsRunning()) {
                System.out.println("SUCCESS");
                queueIsStarted =true;
                return;
            }
            try {
                Thread.sleep(WAIT_INTERVAL_S * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("FAILED!");
        System.out.println("Unable to start Queue");
    }


    private void startGrakn() {
        boolean graknIsRunning = graknIsRunning();
        if(graknIsRunning) {
            System.out.println("Grakn is already running");
            graknIsStarted =true;
        } else {
            graknStartProcess();
        }
    }

    private void graknStartProcess() {
        System.out.print("Starting Grakn...");
        System.out.flush();

        String command = "java -cp " + classpath + " -Dgrakn.dir=" + homePath + " -Dgrakn.conf="+ configPath +" ai.grakn.engine.Grakn > /dev/null 2>&1 &";

        processHandler.executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                command}, null, null);

        String pid = processHandler.getPidFromPsOf(Grakn.class.getName());

        try {
            Files.write(GRAKN_PID,pid.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            System.out.println("Cannot write Grakn PID on a file");
        }

        LocalDateTime init = LocalDateTime.now();
        LocalDateTime timeout = init.plusSeconds(GRAKN_STARTUP_TIMEOUT_S);

        while(LocalDateTime.now().isBefore(timeout)) {
            System.out.print(".");
            System.out.flush();

            String host = graknConfig.getProperty(GraknConfigKey.SERVER_HOST_NAME);
            int port = graknConfig.getProperty(GraknConfigKey.SERVER_PORT);

            if(graknIsRunning() && graknCheckIfReady(host,port,REST.WebPath.STATUS)) {
                System.out.println("SUCCESS");
                graknIsStarted =true;
                return;
            }
            try {
                Thread.sleep(WAIT_INTERVAL_S * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if(Files.exists(GRAKN_PID)) {
            try {
                Files.delete(GRAKN_PID);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println("FAILED!");
        System.out.println("Unable to start Grakn");
    }

    private boolean graknCheckIfReady(String host, int port, String path) {
        try {
            URL siteURL = UriBuilder.fromUri(new SimpleURI(host, port).toURI()).path(path).build().toURL();
            HttpURLConnection connection = (HttpURLConnection) siteURL
                    .openConnection();
            connection.setRequestMethod("GET");
            connection.connect();

            int code = connection.getResponseCode();
            if (code == 200) {
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            return false;
        }
    }

    private void startAll() {
        startStorage();
        queueStart();
        startGrakn();

        if(!graknIsStarted || !queueIsStarted || !storageIsStarted) {
            System.out.println("Please run 'grakn server status' or check the logs located under 'logs' directory.");
        }

    }

    private void stopProcess(Path pidFile) {
        String pid="";
        if(!Files.exists(pidFile)) {
            return;
        }
        try {
            pid = new String(Files.readAllBytes(pidFile), StandardCharsets.UTF_8);
            pid = pid.trim();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (pid.trim().isEmpty()) {
            return;
        }
        processHandler.kill(pid);

        OutputCommand outputCommand;
        do {
            System.out.print(".");
            System.out.flush();

            outputCommand = processHandler.executeAndWait(new String[]{
                    "/bin/sh",
                    "-c",
                    "kill -0 " + pid.trim()
            }, null, null);

            try {
                Thread.sleep(WAIT_INTERVAL_S * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
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

    private void queueStopProcess() {
        String queuePid="";
        if(!Files.exists(QUEUE_PID)) {
            return;
        }
        try {
            byte[] bytes = Files.readAllBytes(QUEUE_PID);
            queuePid = new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (queuePid.isEmpty()) {
            return;
        }

        String queueBin = selectCommand("redis-cli-osx", "redis-cli-linux");

        processHandler.executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                homePath + "/services/redis/" + queueBin + " shutdown"
        }, null, null);

        OutputCommand outputCommand;
        do {
            System.out.print(".");
            System.out.flush();

            outputCommand = processHandler.executeAndWait(new String[]{
                    "/bin/sh",
                    "-c",
                    "kill -0 " + queuePid.trim()
            }, null, null);

            try {
                Thread.sleep(WAIT_INTERVAL_S * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } while (outputCommand.succes());
        System.out.println("SUCCESS");
    }

    private void serverHelp() {
        System.out.println("Usage: grakn server COMMAND\n" +
                "\n" +
                "COMMAND:\n" +
                "start [grakn|queue|storage]  Start Grakn (or optionally, only one of the component)\n" +
                "stop [grakn|queue|storage]   Stop Grakn (or optionally, only one of the component)\n" +
                "status                         Check if Grakn is running\n" +
                "clean                          DANGEROUS: wipe data completely\n" +
                "\n" +
                "Tips:\n" +
                "- Start Grakn with 'grakn server start'\n" +
                "- Start or stop only one component with, e.g. 'grakn server start storage' or 'grakn server stop storage', respectively\n");
    }

    private void graknServerStatus(String verboseFlag) {
        if (storageIsRunning()) {
            System.out.println("Storage: RUNNING");
        } else {
            System.out.println("Storage: NOT RUNNING");
        }

        if (queueIsRunning()) {
            System.out.println("Queue: RUNNING");
        } else {
            System.out.println("Queue: NOT RUNNING");
        }

        if (graknIsRunning()) {
            System.out.println("Grakn: RUNNING");
        } else {
            System.out.println("Grakn: NOT RUNNING");
        }
        if(verboseFlag.equals("--verbose")) {
            System.out.println("======== Failure Diagnostics ========");
            System.out.println("Grakn pid = '"+ processHandler.getPidFromFile(GRAKN_PID).orElse("")+"' (from "+GRAKN_PID+"), '"+getPidOfGrakn()+"' (from ps -ef)");
            System.out.println("Queue pid = '"+ processHandler.getPidFromFile(QUEUE_PID).orElse("")+"' (from "+QUEUE_PID+"), '"+ getPidOfQueue() +"' (from ps -ef)");
            System.out.println("Storage pid = '"+ processHandler.getPidFromFile(STORAGE_PID).orElse("")+"' (from "+STORAGE_PID+"), '"+getPidOfStorage()+"' (from ps -ef)");
        }
    }

    public String getPidOfStorage() {
        return processHandler.getPidFromPsOf("CassandraDaemon");
    }

    public String getPidOfGrakn() {
        return processHandler.getPidFromPsOf(Grakn.class.getName());
    }

    public String getPidOfQueue() {
        return processHandler.getPidFromPsOf("redis-server");
    }

}

