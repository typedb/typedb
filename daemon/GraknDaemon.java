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

package grakn.core.daemon;

import grakn.core.common.config.SystemProperty;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.util.GraknVersion;
import grakn.core.daemon.executor.Executor;
import grakn.core.daemon.executor.Server;
import grakn.core.daemon.executor.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Scanner;

/**
 * The {@link GraknDaemon} class is responsible for starting, stopping and cleaning the keyspaces of Grakn
 */
public class GraknDaemon {
    private static final Logger LOG = LoggerFactory.getLogger(GraknDaemon.class);

    private static final String SERVER = "server";
    private static final String STORAGE = "storage";

    private final Storage storageExecutor;
    private final Server serverExecutor;

    /**
     * Main function of the {@link GraknDaemon}. It is meant to be invoked by the 'grakn' bash script.
     * You should have 'grakn.dir' and 'grakn.conf' Java properties set.
     *
     * @param args arguments such as 'server start', 'server stop', 'clean', and so on
     */
    public static void main(String[] args) {
        try {
            Path graknHome = Paths.get(Objects.requireNonNull(SystemProperty.CURRENT_DIRECTORY.value()));
            Path graknProperties = Paths.get(Objects.requireNonNull(SystemProperty.CONFIGURATION_FILE.value()));

            assertEnvironment(graknHome, graknProperties);

            printGraknLogo();

            Executor bootupProcessExecutor = new Executor();
            GraknDaemon daemon = new GraknDaemon(
                    new Storage(bootupProcessExecutor, graknHome, graknProperties),
                    new Server(bootupProcessExecutor, graknHome, graknProperties)
            );

            daemon.run(args);
            System.exit(0);

        } catch (RuntimeException ex) {
            LOG.error(ErrorMessage.UNABLE_TO_START_GRAKN.getMessage(), ex);
            System.out.println(ErrorMessage.UNABLE_TO_START_GRAKN.getMessage());
            System.err.println(ex.getMessage());
            System.exit(1);
        }
    }

    private GraknDaemon(Storage storageExecutor, Server serverExecutor) {
        this.storageExecutor = storageExecutor;
        this.serverExecutor = serverExecutor;
    }

    /**
     * Basic environment checks. Grakn should only be ran if users are running Java 8,
     * home folder can be detected, and the configuration file 'grakn.properties' exists.
     *
     * @param graknHome       path to $GRAKN_HOME
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
        if (!graknHome.resolve("LICENSE").toFile().exists()) {
            throw new RuntimeException(ErrorMessage.UNABLE_TO_GET_GRAKN_LICENSE.getMessage());
        }
    }

    private static void printGraknLogo() {
        Path ascii = Paths.get(".", "services", "grakn", "grakn-core-ascii.txt");
        if (ascii.toFile().exists()) {
            try {
                System.out.println(new String(Files.readAllBytes(ascii), StandardCharsets.UTF_8));
            } catch (IOException e) {
                // DO NOTHING
            }
        }
    }

    /**
     * Accepts various Grakn commands (eg., 'grakn server start')
     *
     * @param args arrays of arguments, eg., { 'server', 'start' }
     *             option may be eg., `--benchmark`
     */
    public void run(String[] args) {
        String context = args.length > 0 ? args[0] : "";
        String action = args.length > 1 ? args[1] : "";
        String option = args.length > 2 ? args[2] : "";

        switch (context) {
            case "server":
                server(action, option);
                break;
            case "--version":
                version();
                break;
            case "--license":
                license();
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
            case SERVER:
                serverExecutor.stop();
                break;
            case STORAGE:
                storageExecutor.stop();
                break;
            default:
                serverExecutor.stop();
                storageExecutor.stop();
        }
    }

    private void serverStart(String arg) {
        switch (arg) {
            case SERVER:
                serverExecutor.startIfNotRunning(arg);
                break;
            case STORAGE:
                storageExecutor.startIfNotRunning();
                break;
            default:
                storageExecutor.startIfNotRunning();
                serverExecutor.startIfNotRunning(arg);
        }
    }

    private void serverHelp() {
        System.out.println("Usage: grakn server COMMAND\n" +
                                   "\n" +
                                   "COMMAND:\n" +
                                   "start [" + SERVER + "|" + STORAGE + "|--benchmark] Start Grakn (or optionally, only one of the component, or with benchmarking enabled)\n" +
                                   "stop [" + SERVER + "|" + STORAGE + "]   Stop Grakn (or optionally, only one of the component)\n" +
                                   "status                         Check if Grakn is running\n" +
                                   "clean                          DANGEROUS: wipe data completely\n" +
                                   "\n" +
                                   "Tips:\n" +
                                   "- Start Grakn with 'grakn server start'\n" +
                                   "- Start or stop only one component with, e.g. 'grakn server start storage' or 'grakn server stop storage', respectively\n" +
                                   "- Start Grakn with Zipkin-enabled benchmarking with `grakn server start --benchmark`");
    }

    private void serverStatus(String verboseFlag) {
        storageExecutor.status();
        serverExecutor.status();

        if (verboseFlag.equals("--verbose")) {
            System.out.println("======== Failure Diagnostics ========");
            storageExecutor.statusVerbose();
            serverExecutor.statusVerbose();
        }
    }

    private void version() {
        System.out.println(GraknVersion.VERSION);
    }

    private void license() {
        try {
            System.out.println(new String(Files.readAllBytes(Paths.get(".", "LICENSE")), StandardCharsets.UTF_8));
        } catch (IOException e) {
            // shouldn't happen because we checked license existence on bootup
            // if it did, rethrow as runtime exception
            throw new RuntimeException(e);
        }
    }

    private void help() {
        System.out.println("Usage: grakn <COMMAND|OPTION>\n" +
                                   "\n" +
                                   "COMMAND:\n" +
                                   "server     Manage Grakn components\n" +
                                   "help       Print this message\n" +
                                   "\n" +
                                   "OPTION:\n" +
                                   "--version    Print Grakn version\n" +
                                   "--license    Print Grakn license\n" +
                                   "\n" +
                                   "Tips:\n" +
                                   "- Start Grakn with 'grakn server start'\n" +
                                   "- You can then perform queries by opening a console with 'grakn console'");
    }

    private void clean() {
        boolean storage = storageExecutor.isRunning();
        boolean server = serverExecutor.isRunning();
        if (storage || server) {
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
        storageExecutor.clean();
        serverExecutor.clean();
    }
}

