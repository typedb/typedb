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

    private static final String GRAKN_PID="/tmp/grakn.pid";
    private static final String QUEUE_PID="/tmp/grakn-queue.pid";
    private static final String STORAGE_PID="/tmp/grakn-storage.pid";

    private static final long STORAGE_STARTUP_TIMEOUT_S=60;
    private static final long QUEUE_STARTUP_TIMEOUT_S = 10;
    private static final long GRAKN_STARTUP_TIMEOUT_S = 120;

    private static final long WAIT_INTERVAL_S=2;

    private final GraknConfig graknConfig;
    private final String configPath;

    private boolean storageIsStarted;
    private boolean queueIsStarted;
    private boolean graknIsStarted;
    private String classpath;

    private final String homePath;

    private final ProcessHandler processHandler;

    /**
     * In order to run this method you should have 'grakn.dir' and 'grakn.conf' set
     *
     * @param args
     */
    public static void main(String[] args) {
        String homeStatic = GraknSystemProperty.CURRENT_DIRECTORY.value();
        String configStatic = GraknSystemProperty.CONFIGURATION_FILE.value();

        if(homeStatic==null || configStatic==null) {
            System.out.println("Problem with bash script: cannot run Graql");
            return;
        }

        String arg0 = args.length > 0 ? args[0] : "";
        String arg1 = args.length > 1 ? args[1] : "";
        String arg2 = args.length > 2 ? args[2] : "";

        DistGrakn application = new DistGrakn(homeStatic,configStatic, new ProcessHandler());
        try {
            application.run(new String[]{arg0,arg1,arg2});
        } catch (RuntimeException ex) {
            System.out.println(ex.getMessage());
        }

    }

    public void run(String[] args) {
        classpath = processHandler.getClassPathFrom(homePath);

        String arg0 = args.length > 0 ? args[0] : "";
        String arg1 = args.length > 1 ? args[1] : "";
        String arg2 = args.length > 2 ? args[2] : "";

        switch (arg0) {
            case "server":
                server(arg1, arg2);
                break;
            case "version":
                version();
                break;
            default:
                help();
        }
    }

    public DistGrakn(String homePath, String configPath, ProcessHandler processHandler) {
        this.homePath = homePath;
        this.configPath = configPath;
        this.graknConfig = GraknConfig.read(new File(configPath));
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

    private void server(String arg1, String arg2) {
        switch (arg1) {
            case "start":
                graknServerStart(arg2);
                break;
            case "stop":
                graknServerStop(arg2);
                break;
            case "status":
                graknServerStatus(arg2);
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
                Files.delete(Paths.get(homePath,"db","cassandra"));
                Files.createDirectories(Paths.get(homePath,"db","cassandra","data"));
                Files.createDirectories(Paths.get(homePath,"db","cassandra","commitlog"));
                Files.createDirectories(Paths.get(homePath,"db","cassandra","saved_caches"));
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
        OutputCommand operatingSystem = processHandler.executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                "uname"
        },null,null);
        String queueBin = operatingSystem.output.trim().equals("Darwin") ? "redis-cli-osx" : "redis-cli-linux";

        processHandler.executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                homePath +"/services/redis/"+queueBin+" flushall"
        },null,null);


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
            storageStopProcess();
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
            graknStopProcess();
        }
    }

    private void graknServerStart(String arg) {
        switch (arg) {
            case GRAKN: startGrakn();
                break;
            case QUEUE: startQueue();
                break;
            case STORAGE: startStorage();
                break;
            default: startAll();
        }

    }

    private boolean checkIfRunningBy(String pid) {
        boolean isRunning = false;
        String processPid;
        if (Files.exists(Paths.get(pid))) {
            try {
                processPid = new String(Files.readAllBytes(Paths.get(pid)),StandardCharsets.UTF_8);
                if(processPid.trim().isEmpty()) {
                    return false;
                }
                OutputCommand command = processHandler.executeAndWait(new String[]{
                        "/bin/sh",
                        "-c",
                        "ps -p "+processPid.trim()+" | wc -l"
                },null,null);
                return Integer.parseInt(command.output.trim())>1;
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
        if(Files.exists(Paths.get(STORAGE_PID))) {
            try {
                Files.delete(Paths.get(STORAGE_PID));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        OutputCommand outputCommand = processHandler.executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                homePath + "/services/cassandra/cassandra -p " + STORAGE_PID
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

    private void startQueue() {
        queueStart();
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
        OutputCommand operatingSystem = processHandler.executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                "uname"
        },null,null);
        String queueBin = operatingSystem.output.trim().equals("Darwin") ? "redis-server-osx" : "redis-server-linux";

        // run queue
        // queue needs to be ran with $GRAKN_HOME as the working directory
        // otherwise it won't be able to find its data directory located at $GRAKN_HOME/db/redis
        processHandler.executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                homePath +"/services/redis/"+queueBin+" "+ homePath +"/services/redis/redis.conf"
        },null,new File(homePath));

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
            Files.write(Paths.get(GRAKN_PID),pid.getBytes(StandardCharsets.UTF_8));
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

        if(Files.exists(Paths.get(GRAKN_PID))) {
            try {
                Files.delete(Paths.get(GRAKN_PID));
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
        startQueue();
        startGrakn();

        if(!graknIsStarted || !queueIsStarted || !storageIsStarted) {
            System.out.println("Please run 'grakn server status' or check the logs located under 'logs' directory.");
        }

    }

    public void graknStopProcess() {
        String graknPid="";
        if(Files.exists(Paths.get(GRAKN_PID))) {
            try {
                graknPid = new String(Files.readAllBytes(Paths.get(GRAKN_PID)),StandardCharsets.UTF_8);
                graknPid = graknPid.trim();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (graknPid.trim().isEmpty()) {
                return;
            }
            processHandler.kill(graknPid);
        }

        while(true) {
            System.out.print(".");
            System.out.flush();

            OutputCommand outputCommand = processHandler.executeAndWait(new String[]{
                    "/bin/sh",
                    "-c",
                    "kill -0 " + graknPid.trim()
            }, null, null);

            if(outputCommand.exitStatus==0) {
                System.out.println("SUCCESS");
                try {
                    Files.delete(Paths.get(GRAKN_PID));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return;
            }
            try {
                Thread.sleep(WAIT_INTERVAL_S * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void queueStopProcess() {
        String queuePid="";
        if(Files.exists(Paths.get(QUEUE_PID))) {
            try {
                byte[] bytes = Files.readAllBytes(Paths.get(QUEUE_PID));
                queuePid = new String(bytes, StandardCharsets.UTF_8);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (queuePid.isEmpty()) {
                return;
            }
        } else {
            return;
        }

        OutputCommand operatingSystem = processHandler.executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                "uname"
        },null,null);
        String queueBin = operatingSystem.output.trim().equals("Darwin") ? "redis-cli-osx" : "redis-cli-linux";


        processHandler.executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                homePath + "/services/redis/" + queueBin + " shutdown"
        }, null, null);


        while(true) {
            System.out.print(".");
            System.out.flush();

            OutputCommand outputCommand = processHandler.executeAndWait(new String[]{
                    "/bin/sh",
                    "-c",
                    "kill -0 " + queuePid.trim()+" 2>/dev/null"
            }, null, null);

            if(outputCommand.exitStatus>0) {
                System.out.println("SUCCESS");
                return;
            }
            try {
                Thread.sleep(WAIT_INTERVAL_S * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void storageStopProcess() {
        String storagePid="";
        if(Files.exists(Paths.get(STORAGE_PID))) {
            try {
                storagePid = new String(Files.readAllBytes(Paths.get(STORAGE_PID)),StandardCharsets.UTF_8);
                storagePid = storagePid.trim();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (storagePid.trim().isEmpty()) {
                return;
            }
            processHandler.kill(storagePid);
        }

        while(true) {
            System.out.print(".");
            System.out.flush();

            OutputCommand outputCommand = processHandler.executeAndWait(new String[]{
                    "/bin/sh",
                    "-c",
                    "kill -0 " + storagePid.trim()
            }, null, null);

            if(outputCommand.exitStatus==0) {
                System.out.println("SUCCESS");
                try {
                    if(Files.exists(Paths.get(STORAGE_PID))) {
                        Files.delete(Paths.get(STORAGE_PID));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return;
            }
            try {
                Thread.sleep(WAIT_INTERVAL_S * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

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

    private void graknServerStatus(String arg2) {
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
        if(arg2.equals("--verbose")) {
            System.out.println("======== Failure Diagnostics ========");
            System.out.println("Grakn pid = '"+ processHandler.getPidFromFile(GRAKN_PID)+"' (from "+GRAKN_PID+"), '"+getPidOfGrakn()+"' (from ps -ef)");
            System.out.println("Queue pid = '"+ processHandler.getPidFromFile(QUEUE_PID)+"' (from "+QUEUE_PID+"), '"+ getPidOfQueue() +"' (from ps -ef)");
            System.out.println("Storage pid = '"+ processHandler.getPidFromFile(STORAGE_PID)+"' (from "+STORAGE_PID+"), '"+getPidOfStorage()+"' (from ps -ef)");
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

