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
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.graql.GraqlShell;
import ai.grakn.util.REST;
import ai.grakn.util.SimpleURI;

import javax.ws.rs.core.UriBuilder;
import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Scanner;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * @author Michele Orsi
 */
public class DistApplication {

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

    private static GraknEngineConfig GRAKN_CONFIG;
    private static String GRAKN_CONFIG_PATH;

    private boolean storageIsStarted;
    private boolean queueIsStarted;
    private boolean graknIsStarted;

    private final Scanner scanner;
    private final PrintStream output;
    private static String GRAKN_HOME;
    private static String CLASSPATH;

    public static void main(String[] args) {
        GRAKN_HOME = GraknSystemProperty.CURRENT_DIRECTORY.value();
        CLASSPATH = getClassPathFrom(GRAKN_HOME);
        GRAKN_CONFIG = GraknEngineConfig.create();
        GRAKN_CONFIG_PATH = GraknSystemProperty.CONFIGURATION_FILE.value();

        String arg0 = args.length > 0 ? args[0] : "";
        String arg1 = args.length > 1 ? args[1] : "";
        String arg2 = args.length > 2 ? args[2] : "";

        DistApplication application = new DistApplication(new Scanner(System.in, StandardCharsets.UTF_8.name()),System.out);
        application.run(new String[]{arg0,arg1,arg2});
    }

    // TODO: check all the output for failing starting stuff
    // TODO strip out PID line
    // TODO: test clean

    public void run(String[] args) {

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
                defaultChoice();
        }
    }

    public static String getClassPathFrom(String folder){
        FilenameFilter jarFiles = (dir, name) -> name.toLowerCase().endsWith(".jar");
        Stream<File> jars = Stream.of(new File(folder + "/services/lib").listFiles(jarFiles));
        File conf = new File(folder + "/conf/");
        File graknLogback = new File(folder + "/services/grakn/");
        return ":"+Stream.concat(jars, Stream.of(conf, graknLogback))
                .filter(f -> !f.getName().contains("slf4j-log4j12"))
                .map(File::getAbsolutePath)
                .sorted() // we need to sort otherwise it doesn't load logback configuration
                .collect(joining(":"));
    }


    public DistApplication(Scanner scanner, PrintStream output) {
        this.scanner = scanner;
        this.output = output;
    }

    private void version() {
        GraqlShell.main(new String[]{"--v"});
    }

    private void defaultChoice() {
        output.println("Usage: grakn COMMAND\n" +
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
        boolean storage = storageCheckIfRunning();
        boolean queue = queueCheckIfRunning();
        boolean grakn = graknCheckIfRunning();
        if(storage || queue || grakn) {
            output.println("Grakn is still running! Please do a shutdown with 'grakn server stop' before performing a cleanup.");
        } else {
            output.print("Are you sure you want to delete all stored data and logs? [y/N] ");
            output.flush();
            String response = scanner.next();
            if(!response.equals("y") && !response.equals("Y")) {
                output.println("Response '"+response+"' did not equal 'y' or 'Y'.  Canceling clean operation.");
                return;
            }
            output.print("Cleaning Storage...");
            output.flush();
            try {
                Files.delete(Paths.get(GRAKN_HOME+"db/cassandra"));
                Files.createDirectories(Paths.get(GRAKN_HOME+"db/cassandra/data"));
                Files.createDirectories(Paths.get(GRAKN_HOME+"db/cassandra/commitlog"));
                Files.createDirectories(Paths.get(GRAKN_HOME+"db/cassandra/saved_caches"));
                output.println("SUCCESS");
            } catch (IOException e) {
                output.println("FAILED!");
                output.println("Unable to clean Storage");
            }

            output.print("Cleaning Queue...");
            output.flush();
            queueStart();
            queueWipeAllData();
            stopQueue();
            output.println("SUCCESS");

            output.print("Cleaning Grakn...");
            output.flush();
            try {
                Files.delete(Paths.get(GRAKN_HOME+"logs"));
                Files.createDirectories(Paths.get(GRAKN_HOME+"logs"));
                output.println("SUCCESS");
            } catch (IOException e) {
                output.println("FAILED!");
                output.println("Unable to clean Grakn");
            }

        }
    }

    private void queueWipeAllData() {
        OutputCommand operatingSystem = executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                "uname"
        },null,null);
        String queueBin = operatingSystem.output.trim().equals("Darwin") ? "redis-cli-osx" : "redis-cli-linux";

        executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                GRAKN_HOME+"/services/redis/"+queueBin+" flushall"
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
        output.print("Stopping Storage...");
        output.flush();
        boolean storageIsRunning = storageCheckIfRunning();
        if(!storageIsRunning) {
            output.println("NOT RUNNING");
        } else {
            storageStopProcess();
        }
    }

    public void stopQueue() {
        output.print("Stopping Queue...");
        output.flush();
        boolean queueIsRunning = queueCheckIfRunning();
        if(!queueIsRunning) {
            output.println("NOT RUNNING");
        } else {
            queueStopProcess();
        }
    }

    public void stopGrakn() {
        output.print("Stopping Grakn...");
        output.flush();
        boolean graknIsRunning = graknCheckIfRunning();
        if(!graknIsRunning) {
            output.println("NOT RUNNING");
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
                OutputCommand command = executeAndWait(new String[]{
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

    private boolean storageCheckIfRunning() {
        return checkIfRunningBy(STORAGE_PID);
    }

    private boolean queueCheckIfRunning() {
        return checkIfRunningBy(QUEUE_PID);
    }

    private boolean graknCheckIfRunning() {
        return checkIfRunningBy(GRAKN_PID);
    }

    private void startStorage() {
        boolean storageIsRunning = storageCheckIfRunning();
        if(storageIsRunning) {
            output.println("Storage is already running");
            storageIsStarted =true;
        } else {
            storageStartProcess();
        }
    }

    private void storageStartProcess() {
        output.print("Starting Storage...");
        output.flush();
        OutputCommand outputCommand = executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                GRAKN_HOME + "/services/cassandra/cassandra -p " + STORAGE_PID
        }, null, null);
        LocalDateTime init = LocalDateTime.now();
        LocalDateTime timeout = init.plusSeconds(STORAGE_STARTUP_TIMEOUT_S);

        while(LocalDateTime.now().isBefore(timeout) && outputCommand.exitStatus<1) {
            output.print(".");
            output.flush();

            OutputCommand storageStatus = executeAndWait(new String[]{
                    "/bin/sh",
                    "-c",
                    GRAKN_HOME + "/services/cassandra/nodetool statusthrift 2>/dev/null | tr -d '\n\r'"
            },null,null);
            if(storageStatus.output.trim().equals("running")) {
                output.println("SUCCESS");
                storageIsStarted =true;
                return;
            }
            try {
                Thread.sleep(WAIT_INTERVAL_S*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        output.println("FAILED!");
        output.println("Unable to start Storage");
    }

    private void startQueue() {
        queueStart();
    }

    private void queueStart() {
        boolean queueRunning = queueCheckIfRunning();
        if(queueRunning) {
            output.println("Queue is already running");
            queueIsStarted =true;
        } else {
            queueStartProcess();
        }
    }

    private void queueStartProcess() {
        output.print("Starting Queue...");
        output.flush();
        OutputCommand operatingSystem = executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                "uname"
        },null,null);
        String queueBin = operatingSystem.output.trim().equals("Darwin") ? "redis-server-osx" : "redis-server-linux";

        // run queue
        // queue needs to be ran with $GRAKN_HOME as the working directory
        // otherwise it won't be able to find its data directory located at $GRAKN_HOME/db/redis
        executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                GRAKN_HOME+"/services/redis/"+queueBin+" "+GRAKN_HOME+"/services/redis/redis.conf"
        },null,new File(GRAKN_HOME));

        LocalDateTime init = LocalDateTime.now();
        LocalDateTime timeout = init.plusSeconds(QUEUE_STARTUP_TIMEOUT_S);

        while(LocalDateTime.now().isBefore(timeout)) {
            output.print(".");
            output.flush();

            if(queueCheckIfRunning()) {
                output.println("SUCCESS");
                queueIsStarted =true;
                return;
            }
            try {
                Thread.sleep(WAIT_INTERVAL_S * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        output.println("FAILED!");
        output.println("Unable to start Queue");
    }


    private void startGrakn() {
        boolean graknIsRunning = graknCheckIfRunning();
        if(graknIsRunning) {
            output.println("Grakn is already running");
            graknIsStarted =true;
        } else {
            graknStartProcess();
        }
    }

    private void graknStartProcess() {
        output.print("Starting Grakn...");
        output.flush();

        String command = "java -cp " + CLASSPATH + " -Dgrakn.dir=" + GRAKN_HOME + " -Dgrakn.conf="+GRAKN_CONFIG_PATH+" ai.grakn.engine.Grakn &";

        executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                command}, null, null);

        String pid = getPidOf(Grakn.class.getName());

        try {
            Files.write(Paths.get(GRAKN_PID),pid.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            output.println("Cannot write Grakn PID on a file");
        }

        LocalDateTime init = LocalDateTime.now();
        LocalDateTime timeout = init.plusSeconds(GRAKN_STARTUP_TIMEOUT_S);

        while(LocalDateTime.now().isBefore(timeout)) {
            output.print(".");
            output.flush();

            String host = GRAKN_CONFIG.getProperty(GraknConfigKey.SERVER_HOST_NAME);
            int port = GRAKN_CONFIG.getProperty(GraknConfigKey.SERVER_PORT);

            if(graknCheckIfRunning() && graknCheckIfReady(host,port,REST.WebPath.System.STATUS)) {
                output.println("SUCCESS");
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

        output.println("FAILED!");
        output.println("Unable to start Grakn");
    }

    private String getPidOf(String processName) {
        return executeAndWait(new String[]{
                    "/bin/sh",
                    "-c",
                    "ps -ef | grep " + processName + " | grep -v grep | awk '{ print $2}' "
            }, null, null).output;
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
            output.println("Unable to start Grakn. Please run 'grakn server status' or check the logs located under 'logs' directory.");
        }

    }

    public void graknStopProcess() {
        String graknPid="";
        if(Files.exists(Paths.get(GRAKN_PID))) {
            try {
                graknPid = new String(Files.readAllBytes(Paths.get(GRAKN_PID)),StandardCharsets.UTF_8);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (graknPid.trim().isEmpty()) {
                return;
            }
            executeAndWait(new String[]{
                    "/bin/sh",
                    "-c",
                    "kill " + graknPid.trim()
            }, null, null);
        }

        while(true) {
            output.print(".");
            output.flush();

            OutputCommand outputCommand = executeAndWait(new String[]{
                    "/bin/sh",
                    "-c",
                    "kill -0 " + graknPid.trim()
            }, null, null);

            if(outputCommand.exitStatus==0) {
                output.println("SUCCESS");
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

        OutputCommand operatingSystem = executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                "uname"
        },null,null);
        String queueBin = operatingSystem.output.trim().equals("Darwin") ? "redis-cli-osx" : "redis-cli-linux";


        executeAndWait(new String[]{
                "/bin/sh",
                "-c",
                GRAKN_HOME + "/services/redis/" + queueBin + " shutdown"
        }, null, null);


        while(true) {
            output.print(".");
            output.flush();

            OutputCommand outputCommand = executeAndWait(new String[]{
                    "/bin/sh",
                    "-c",
                    "kill -0 " + queuePid.trim()+" 2>/dev/null"
            }, null, null);

            if(outputCommand.exitStatus>0) {
                output.println("SUCCESS");
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
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (storagePid.trim().isEmpty()) {
                return;
            }
            executeAndWait(new String[]{
                    "/bin/sh",
                    "-c",
                    "kill " + storagePid.trim()
            }, null, null);
        }

        while(true) {
            output.print(".");
            output.flush();

            OutputCommand outputCommand = executeAndWait(new String[]{
                    "/bin/sh",
                    "-c",
                    "kill -0 " + storagePid.trim()
            }, null, null);

            if(outputCommand.exitStatus==0) {
                output.println("SUCCESS");
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
        output.println("Usage: grakn server COMMAND\n" +
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
        if (storageCheckIfRunning()) {
            output.println("Storage: RUNNING");
        } else {
            output.println("Storage: NOT RUNNING");
        }

        if (queueCheckIfRunning()) {
            output.println("Queue: RUNNING");
        } else {
            output.println("Queue: NOT RUNNING");
        }

        if (graknCheckIfRunning()) {
            output.println("Grakn: RUNNING");
        } else {
            output.println("Grakn: NOT RUNNING");
        }
        if(arg2.equals("--verbose")) {
            output.println("======== Failure Diagnostics ========");
            output.println("Grakn pid = '"+ getPidFromFile(GRAKN_PID)+"' (from "+GRAKN_PID+"), '"+getPidOfGrakn()+"' (from ps -ef)");
            output.println("Queue pid = '"+ getPidFromFile(QUEUE_PID)+"' (from "+QUEUE_PID+"), '"+ getPidOfQueue() +"' (from ps -ef)");
            output.println("Storage pid = '"+ getPidFromFile(STORAGE_PID)+"' (from "+STORAGE_PID+"), '"+getPidOfStorage()+"' (from ps -ef)");
        }
    }

    public String getPidOfStorage() {
        return getPidOf("CassandraDaemon");
    }

    public String getPidOfGrakn() {
        return getPidOf(Grakn.class.getName());
    }

    public String getPidOfQueue() {
        return getPidOf("redis-server");
    }

    private String getPidFromFile(String fileName) {
        String pid="";
        if (Files.exists(Paths.get(fileName))) {
            try {
                pid = new String(Files.readAllBytes(Paths.get(fileName)),StandardCharsets.UTF_8).trim();
            } catch (IOException e) {
                // DO NOTHING
            }
        }
        return pid;
    }

    public OutputCommand executeAndWait(String[] cmdarray, String[] envp, File dir) {

        StringBuffer outputS = new StringBuffer();
        int exitValue = 1;

        Process p;
        BufferedReader reader = null;
        try {
            p = Runtime.getRuntime().exec(cmdarray, envp, dir);
            p.waitFor();
            exitValue = p.exitValue();
            reader =
                    new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));

            String line = "";
            while ((line = reader.readLine()) != null) {
                outputS.append(line + "\n");
            }

        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        OutputCommand outputCommand = new OutputCommand(outputS.toString().trim(), exitValue);
        return outputCommand;
    }

    static class OutputCommand {
        final String output;
        final int exitStatus;

        OutputCommand(String output, int exitStatus) {
            this.output = output;
            this.exitStatus = exitStatus;
        }

        @Override
        public String toString() {
            return "OutputCommand{" +
                    "output='" + output + '\'' +
                    ", exitStatus=" + exitStatus +
                    '}';
        }
    }
}

