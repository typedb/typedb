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

import ai.grakn.GraknSystemProperty;
import ai.grakn.dist.lock.Lock;
import ai.grakn.dist.lock.LockAlreadyAcquiredException;
import ai.grakn.dist.lock.MkdirLock;
import ai.grakn.util.GraknVersion;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

/**
 * 
 * @author Michele Orsi
 */
public class DistGrakn {
    private static final String SYNCHRONIZED_BOOTUP_LOCK_PATH = "/tmp/grakn-synchronized-bootup.lock";

    private static final String GRAKN = "grakn";
    private static final String QUEUE = "queue";
    private static final String STORAGE = "storage";

    private final StorageProcess storageProcess;
    private final QueueProcess queueProcess;
    private final GraknProcess graknProcess;
    private final Lock synchronizedBootupLock;


    /**
     * Invocation from bash script 'grakn'
     * In order to run this method you should have 'grakn.dir' and 'grakn.conf' set
     *
     * @param args
     */
    public static void main(String[] args) {
        printAscii();

        try {
            Path homeStatic = Paths.get(GraknSystemProperty.CURRENT_DIRECTORY.value());
            Path configStatic = Paths.get(GraknSystemProperty.CONFIGURATION_FILE.value());

            if(!Files.exists(homeStatic.resolve("grakn"))) {
                throw new RuntimeException("Cannot find home folder");
            }
            if(!Files.exists(homeStatic)) {
                throw new RuntimeException("Cannot find config folder");
            }

            newDistGrakn(homeStatic, configStatic).run(args);

        } catch (LockAlreadyAcquiredException e) {
            System.out.println("grakn server start, stop or clean is already in progress. If this isn't the case, it is possible that it has crashed. " +
                    "In that case please make sure to remove the directory " + SYNCHRONIZED_BOOTUP_LOCK_PATH + " before re-attempting.");
        } catch (RuntimeException ex) {
            System.out.println("Problem with bash script: cannot run Grakn");
        }
    }

    private static DistGrakn newDistGrakn(Path homePathFolder, Path configPath) {
        return new DistGrakn(new StorageProcess(homePathFolder),
                new QueueProcess(homePathFolder),
                new GraknProcess(homePathFolder, configPath),
                new MkdirLock(SYNCHRONIZED_BOOTUP_LOCK_PATH));
    }

    public void run(String[] args) {
        String context = args.length > 0 ? args[0] : "";
        String action = args.length > 1 ? args[1] : "";
        String option = args.length > 2 ? args[2] : "";

        switch (context) {
            case "server":
                synchronizedBootupLock.withLock(() -> server(action, option));
                break;
            case "version":
                version();
                break;
            default:
                help();
        }
    }

    public static void printAscii() {
        Path ascii = Paths.get(".", "services", "grakn", "grakn-ascii.txt");
        if(Files.exists(ascii)) {
            try {
                System.out.println(new String(Files.readAllBytes(ascii), StandardCharsets.UTF_8));
            } catch (IOException e) {
                // DO NOTHING
            }
        }
    }

    public DistGrakn(StorageProcess storageProcess, QueueProcess queueProcess, GraknProcess graknProcess, Lock synchronizedBootupLock) {
        this.storageProcess = storageProcess;
        this.queueProcess = queueProcess;
        this.graknProcess = graknProcess;
        this.synchronizedBootupLock = synchronizedBootupLock;
    }

    private void version() {
        System.out.println(GraknVersion.VERSION);
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
                serverStart(option);
                break;
            case "stop":
                serverStop(option);
                break;
            case "status":
                serverStatus(option);
                break;
            case "clean":
                clean();
                break;
            default:
                serverHelp();
        }
    }

    private void clean() {
        boolean storage = storageProcess.isRunning();
        boolean queue = queueProcess.isRunning();
        boolean grakn = graknProcess.isRunning();
        if(storage || queue || grakn) {
            System.out.println("Grakn is still running! Please do a shutdown with 'grakn server stop' before performing a cleanup.");
            return;
        }
        System.out.print("Are you sure you want to delete all stored data and logs? [y/N] ");
        System.out.flush();
        String response = new Scanner(System.in, StandardCharsets.UTF_8.name()).next();
        if (!response.equals("y") && !response.equals("Y")) {
            System.out.println("Response '" + response + "' did not equal 'y' or 'Y'.  Canceling clean operation.");
            return;
        }
        storageProcess.clean();
        queueProcess.clean();
        graknProcess.clean();
    }

    private void serverStop(String arg) {
        switch (arg) {
            case GRAKN:
                graknProcess.stop();
                break;
            case QUEUE:
                queueProcess.stop();
                break;
            case STORAGE:
                storageProcess.stop();
                break;
            default:
                graknProcess.stop();
                queueProcess.stop();
                storageProcess.stop();
        }
    }

    private void serverStart(String arg) {
        switch (arg) {
            case GRAKN:
                try {
                    graknProcess.start();
                } catch (ProcessNotStartedException e) {
                    // DO NOTHING
                }
                break;
            case QUEUE:
                try {
                    queueProcess.start();
                } catch (ProcessNotStartedException e) {
                    // DO NOTHING
                }
                break;
            case STORAGE:
                try {
                    storageProcess.start();
                } catch (ProcessNotStartedException e) {
                    // DO NOTHING
                }
                break;
            default:
                try {
                    storageProcess.start();
                    queueProcess.start();
                    graknProcess.start();
                } catch (ProcessNotStartedException e) {
                    System.out.println("Please run 'grakn server status' or check the logs located under 'logs' directory.");
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

    private void serverStatus(String verboseFlag) {
        storageProcess.status();
        queueProcess.status();
        graknProcess.status();

        if(verboseFlag.equals("--verbose")) {
            System.out.println("======== Failure Diagnostics ========");
            storageProcess.statusVerbose();
            queueProcess.statusVerbose();
            graknProcess.statusVerbose();
        }
    }

}

