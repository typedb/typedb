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

import ai.grakn.GraknSystemProperty;
import ai.grakn.bootup.config.ConfigProcessor;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.GraknVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

/**
 * The {@link GraknBootup} class is responsible for starting, stopping and cleaning the keyspaces of Grakn
 *
 * @author Ganeshwara Herawan Hananda
 * @author Michele Orsi
 */
public class GraknBootup {
    private static final Logger LOG = LoggerFactory.getLogger(GraknBootup.class);

    private static final String ENGINE = "engine";
    private static final String QUEUE = "queue";
    private static final String STORAGE = "storage";

    private final StorageBootup storageBootup;
    private final QueueProcess queueProcess;
    private final EngineProcess engineProcess;

    private GraknBootup(StorageBootup storageBootup, QueueProcess queueProcess, EngineProcess engineProcess) {
        this.storageBootup = storageBootup;
        this.queueProcess = queueProcess;
        this.engineProcess = engineProcess;
    }

    /**
     * Main function of the {@link GraknBootup}. It is meant to be invoked by the 'grakn' bash script
     * You should have 'grakn.dir' and 'grakn.conf' Java properties set
     *
     * @param args
     */
    public static void main(String[] args) {
        try {
            Path graknHome = Paths.get(GraknSystemProperty.CURRENT_DIRECTORY.value());
            Path graknProperties = Paths.get(GraknSystemProperty.CONFIGURATION_FILE.value());

            assertEnvironment(graknHome, graknProperties);

            printAscii();
            newGraknBootup(graknHome, graknProperties).run(args);
        } catch (RuntimeException ex) {
            LOG.error("An error has occurred during boot-up.", ex);
            System.err.println(ex.getMessage());
            System.exit(1);
        }
    }

    /**
     * Basic environment checks. Grakn should only be ran if users are running Java 8,
     * home folder can be detected, and the configuration file 'grakn.properties' exists.
     * @param graknHome path to GRAKN_HOME
     * @param graknProperties path to the 'grakn.properties' file
     */
    private static void assertEnvironment(Path graknHome, Path graknProperties) {
        String javaVersion = System.getProperty("java.specification.version");
        if (!javaVersion.equals("1.8")) {
            throw new RuntimeException(ErrorMessage.UNSUPPORTED_JAVA_VERSION.getMessage(javaVersion));
        }
        if (!graknHome.resolve("grakn").toFile().exists()) {
            throw new RuntimeException(ErrorMessage.UNABLE_TO_GET_GRAKN_HOME_FOLDER.getMessage());
        }
        if (!graknProperties.toFile().exists()) {
            throw new RuntimeException(ErrorMessage.UNABLE_TO_GET_GRAKN_CONFIG_FOLDER.getMessage());
        }
    }

    private static GraknBootup newGraknBootup(Path graknHome, Path configPath) {
        return new GraknBootup(
                new StorageBootup(graknHome, configPath),
                new QueueProcess(graknHome),
                new EngineProcess(graknHome, configPath));
    }

    private static void printAscii() {
        Path ascii = Paths.get(".", "services", "grakn", "grakn-ascii.txt");
        if(ascii.toFile().exists()) {
            try {
                System.out.println(new String(Files.readAllBytes(ascii), StandardCharsets.UTF_8));
            } catch (IOException e) {
                // DO NOTHING
            }
        }
    }

    /**
     * Accepts various Grakn commands (eg., 'grakn server start')
     * @param args arrays of arguments, eg., { 'server', 'start' }
     */
    public void run(String[] args) {
        String context = args.length > 0 ? args[0] : "";
        String action = args.length > 1 ? args[1] : "";
        String option = args.length > 2 ? args[2] : "";

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

    private void serverStop(String arg) {
        switch (arg) {
            case ENGINE:
                engineProcess.stop();
                break;
            case QUEUE:
                queueProcess.stop();
                break;
            case STORAGE:
                storageBootup.stop();
                break;
            default:
                engineProcess.stop();
                queueProcess.stop();
                storageBootup.stop();
        }
    }

    private void serverStart(String arg) {
        switch (arg) {
            case ENGINE:
                try {
                    engineProcess.startIfNotRunning();
                } catch (ProcessNotStartedException e) {
                    // DO NOTHING
                }
                break;
            case QUEUE:
                try {
                    queueProcess.startIfNotRunning();
                } catch (ProcessNotStartedException e) {
                    // DO NOTHING
                }
                break;
            case STORAGE:
                try {
                    storageBootup.startIfNotRunning();
                } catch (ProcessNotStartedException e) {
                    // DO NOTHING
                }
                break;
            default:
                try {
                    ConfigProcessor.updateProcessConfigs();
                    storageBootup.startIfNotRunning();
                    queueProcess.startIfNotRunning();
                    engineProcess.startIfNotRunning();
                } catch (ProcessNotStartedException e) {
                    System.out.println("Please run 'grakn server status' or check the logs located under 'logs' directory.");
                }
        }
    }

    private void serverHelp() {
        System.out.println("Usage: grakn server COMMAND\n" +
                "\n" +
                "COMMAND:\n" +
                "start ["+ENGINE+"|"+QUEUE+"|"+STORAGE+"]  Start Grakn (or optionally, only one of the component)\n" +
                "stop ["+ENGINE+"|"+QUEUE+"|"+STORAGE+"]   Stop Grakn (or optionally, only one of the component)\n" +
                "status                         Check if Grakn is running\n" +
                "clean                          DANGEROUS: wipe data completely\n" +
                "\n" +
                "Tips:\n" +
                "- Start Grakn with 'grakn server start'\n" +
                "- Start or stop only one component with, e.g. 'grakn server start storage' or 'grakn server stop storage', respectively\n");
    }

    private void serverStatus(String verboseFlag) {
        storageBootup.status();
        queueProcess.status();
        engineProcess.status();

        if(verboseFlag.equals("--verbose")) {
            System.out.println("======== Failure Diagnostics ========");
            storageBootup.statusVerbose();
            queueProcess.statusVerbose();
            engineProcess.statusVerbose();
        }
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

    private void clean() {
        boolean storage = storageBootup.isRunning();
        boolean queue = queueProcess.isRunning();
        boolean grakn = engineProcess.isRunning();
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
        storageBootup.clean();
        queueProcess.clean();
        engineProcess.clean();
    }
}

