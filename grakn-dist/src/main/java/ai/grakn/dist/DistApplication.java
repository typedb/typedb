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

import ai.grakn.engine.Grakn;
import ai.grakn.graql.GraqlShell;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Arrays;

/**
 * @author Michele Orsi
 */
public class DistApplication {

    private static final String GRAKN = "grakn";
    private static final String QUEUE = "queue";
    private static final String STORAGE = "storage";

    private static final String GRAKN_PID="/tmp/grakn.pid";
    private static final String STORAGE_PID="/tmp/grakn-storage.pid";
    private static final long STORAGE_STARTUP_TIMEOUT_S=60;
    private static final long QUEUE_STARTUP_TIMEOUT_S = 10;
    private static final long GRAKN_STARTUP_TIMEOUT_S = 120;
    private static final long WAIT_INTERVAL_S=2;

    private static final String GRAKN_CONFIG="../conf/grakn.properties";

    private static boolean storageStarted;
    private static boolean queueStarted;
    private static boolean graknStarted;

    private final PrintStream output;
    private static String GRAKN_HOME;
    private static String CLASSPATH;

    public static void main(String[] args) {
        if(args.length<2) throw new RuntimeException("Errors in 'grakn' bash script");


        GRAKN_HOME = args[0];
        CLASSPATH = args[1];

        String arg2 = args.length > 2 ? args[2] : "";
        String arg3 = args.length > 3 ? args[3] : "";
        String arg4 = args.length > 4 ? args[4] : "";

        DistApplication application = new DistApplication(System.out);
        application.run(new String[]{arg2,arg3,arg4});
    }

    // TODO: check all the output for failing starting stuff

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

    public DistApplication(PrintStream output) {
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
                cli_case_grakn_server_start(arg2);
                break;
            case "stop":
                break;
            case "status":
                cli_case_grakn_server_status();
                break;
            case "clean":
                break;
            default:
                serverHelp();
        }
    }

    private void cli_case_grakn_server_start(String arg) {
        switch (arg) {
            case GRAKN: cli_case_grakn_server_start_grakn();
                break;
            case QUEUE: cli_case_grakn_server_start_queue();
                break;
            case STORAGE: cli_case_grakn_server_start_storage();
                break;
            default: cli_case_grakn_server_start_all();
        }

    }

    private boolean storage_check_if_running_by_pidfile() {
        boolean isRunning = false;
        String storagePid;
        if (Files.exists(Paths.get(STORAGE_PID))) {
            try {
                storagePid = new String(Files.readAllBytes(Paths.get(STORAGE_PID)));
                if(storagePid.trim().isEmpty()) {
                    return false;
                }
                OutputCommand command = executeAndWait(new String[]{
                        "/bin/sh",
                        "-c",
                        "ps -p "+storagePid.trim()+" | wc -l"
                },null,null);
                return Integer.parseInt(command.output.trim())>1;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return isRunning;
    }

    private void cli_case_grakn_server_start_storage() {
        boolean storageIsRunning = storage_check_if_running_by_pidfile();
        if(storageIsRunning) {
            output.println("Storage is already running");
            storageStarted=true;
        } else {
            storage_start_process();
        }
    }

    private void storage_start_process() {
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
                storageStarted=true;
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

    private void cli_case_grakn_server_start_queue() {
        queue_start_and_wait_until_ready();
//        output.println("Unable to start Queue. Please run 'grakn server status' or check the logs located under 'logs' directory.");
//         TODO: print_failure_diagnostics
    }

    private void queue_start_and_wait_until_ready() {
        boolean queueRunning = queue_check_if_running_by_ps_ef();
        if(queueRunning) {
            output.println("Queue is already running");
            queueStarted=true;
        } else {
            queue_start_process();
        }
    }

    private void queue_start_process() {
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

            if(queue_check_if_running_by_ps_ef()) {
                output.println("SUCCESS");
                queueStarted=true;
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


    private void cli_case_grakn_server_start_grakn() {
        grakn_start_and_wait_until_ready();
    }

    private void grakn_start_and_wait_until_ready() {
        boolean graknIsRunning = grakn_check_if_running_by_pidfile();
        if(graknIsRunning) {
            output.println("Grakn is already running");
            graknStarted=true;
        } else {
            grakn_start_process();
        }
    }

    private void grakn_start_process() {
        output.print("Starting Grakn...");
        output.flush();

        executeAndWait(new String[] {
                "/bin/sh",
                "-c",
                "java -cp "+CLASSPATH+" -Dgrakn.dir="+GRAKN_HOME+"/services -Dgrakn.conf="+ GRAKN_HOME+GRAKN_CONFIG +" "+Grakn.class.getName()+" > /dev/null @>&1 &"}, null, null);

    }

    private boolean grakn_check_if_running_by_pidfile() {
        boolean isRunning = false;
        String graknPid;
        if (Files.exists(Paths.get(GRAKN_PID))) {
            try {
                graknPid = new String(Files.readAllBytes(Paths.get(GRAKN_PID)));
                if(graknPid.trim().isEmpty()) {
                    return false;
                }
                OutputCommand command = executeAndWait(new String[]{
                        "/bin/sh",
                        "-c",
                        "ps -p "+graknPid.trim()+" | wc -l"
                },null,null);
                return Integer.parseInt(command.output.trim())>1;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return isRunning;

    }

    private void cli_case_grakn_server_start_all() {
        cli_case_grakn_server_start_storage();
        cli_case_grakn_server_start_queue();
        cli_case_grakn_server_start_grakn();

//        if(!graknStarted || !queueStarted || !storageStarted) {
//            output.println("Unable to start Grakn. Please run 'grakn server status' or check the logs located under 'logs' directory.");
//             TODO: print_failure_diagnostics
//        }

    }

    private void serverStop(String arg) {
        output.println("");
        // TODO: handle if graceful termination failed and log failures
//        switch (arg) {
//            case GRAKN:
//                grakn_stop_process();
//                break;
//            case QUEUE:
//                queue_stop_process();
//                break;
//            case STORAGE:
//                storage_stop_process();
//                break;
//            default:
//                grakn_server_stop_all();
//        }
    }

    private void grakn_server_stop_all() {

    }

    private void grakn_stop_process() {
        output.println("Stopping Grakn...");
        if (numberOfProcessOf("Grakn") > 0) {  // TODO: use PID
            String[] cmd = {
                    "/bin/sh",
                    "-c",
                    "kill `cat $GRAKN_PID`"
            };
            output.println(Arrays.toString(cmd)); // TODO: remove
        } else {
            output.println("Grakn: NOT RUNNING");
        }

//            if [[ $is_grakn_running -eq 0 ]]; then
//            if [[ -e "$GRAKN_PID" ]]; then
//            kill `cat $GRAKN_PID`
//            status_kill=$?
//                    rm $GRAKN_PID
//            print_status_message ${status_kill}
//            return $status_kill
//    else
//            echo "FAILED!" # unable to find PID file
//            return 1
//            fi
//  else
//            echo "NOT RUNNING"
//            fi
//        }
    }

    private void queue_stop_process() {

    }

    private void storage_stop_process() {

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

    private void cli_case_grakn_server_status() {
        if (storage_check_if_running_by_pidfile()) {
            output.println("Storage: RUNNING");
        } else {
            output.println("Storage: NOT RUNNING");
        }

        if (queue_check_if_running_by_ps_ef()) {
            output.println("Queue: RUNNING");
        } else {
            output.println("Queue: NOT RUNNING");
        }

        if (numberOfProcessOf("Grakn") > 0) {  // TODO: perform a real call to server:port/configuration
            output.println("Grakn: RUNNING");
        } else {
            output.println("Grakn: NOT RUNNING");
        }
        // TODO: case $1 --verbose print_failure_diagnostics
    }

    private boolean queue_check_if_running_by_ps_ef() {
        return numberOfProcessOf("redis-server") > 0;
    }

    private int numberOfProcessOf(String command) {
        String[] cmd = {
                "/bin/sh",
                "-c",
                "ps -ef | grep '" + command + "' | grep -v grep | awk '{ print $2}' | wc -l"
        };

        OutputCommand result = executeAndWait(cmd,null,null);
        return Integer.parseInt(result.output.trim());
    }

    private OutputCommand executeAndWait(String[] cmdarray, String[] envp, File dir) {

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

            BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream())); // TODO: remove
            while((line = error.readLine()) != null){
                System.out.println(line);
            }
            error.close();

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
        return new OutputCommand(outputS.toString(),exitValue);
    }

    class OutputCommand {
        final String output;
        final int exitStatus;

        OutputCommand(String output, int exitStatus) {
            this.output = output;
            this.exitStatus = exitStatus;
        }
    }
}

