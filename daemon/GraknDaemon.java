/*
 * Copyright (C) 2020 Grakn Labs
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
import grakn.core.daemon.executor.Executor;
import grakn.core.daemon.executor.Server;
import grakn.core.daemon.executor.Storage;
import grakn.core.server.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;

/**
 * The GraknDaemon class is responsible for starting, stopping and cleaning the keyspaces of Grakn
 */
public class GraknDaemon {
    private static final Logger LOG = LoggerFactory.getLogger(GraknDaemon.class);

    private static final String VERSION_LABEL = "Version: ";

    private final Storage storageExecutor;
    private final Server serverExecutor;

    /**
     * Main function of the GraknDaemon. It is meant to be invoked by the 'grakn' bash script.
     * You should have 'grakn.dir' and 'grakn.conf' Java properties set.
     *
     * @param args arguments such as 'server start', 'server stop', 'clean', and so on
     */
    public static void main(String[] args) {
        String commandName = args[0];
        String[] realArgs = Arrays.copyOfRange(args, 1, args.length);
        System.exit(GraknDaemon.buildCommand(commandName).execute(realArgs));
    }

    private GraknDaemon(boolean showLogo) {
        try {
            Path graknHome = Paths.get(Objects.requireNonNull(SystemProperty.CURRENT_DIRECTORY.value()));
            Path graknProperties = Paths.get(Objects.requireNonNull(SystemProperty.CONFIGURATION_FILE.value()));
            assertEnvironment(graknHome, graknProperties);

            Executor bootupProcessExecutor = new Executor();
            storageExecutor = new Storage(bootupProcessExecutor, graknHome, graknProperties);
            serverExecutor = new Server(bootupProcessExecutor, graknHome, graknProperties);

            if (showLogo) {
                printGraknLogo();
            }
        } catch (RuntimeException ex) {
            LOG.error(ErrorMessage.UNABLE_TO_START_GRAKN.getMessage(), ex);
            System.out.println(ErrorMessage.UNABLE_TO_START_GRAKN.getMessage());
            throw ex;
        }
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
        if (!graknHome.resolve("server").resolve("conf").toFile().exists()) {
            throw new RuntimeException(ErrorMessage.UNABLE_TO_GET_GRAKN_HOME_FOLDER.getMessage());
        }
        if (!graknProperties.toFile().exists()) {
            throw new RuntimeException(ErrorMessage.UNABLE_TO_GET_GRAKN_CONFIG_FOLDER.getMessage());
        }
    }

    private static void printGraknLogo() {
        Path ascii = Paths.get(".", "server", "services", "grakn", "grakn-core-ascii.txt");
        if (ascii.toFile().exists()) {
            try {
                String logoString = new String(Files.readAllBytes(ascii), StandardCharsets.UTF_8);
                int lineLength = logoString.split("\n")[0].length();
                int spaces = lineLength - (VERSION_LABEL.length() + Version.VERSION.length()) - 1;

                System.out.println(logoString);
                if (spaces > 0) {
                    char[] charSpaces = new char[spaces];
                    Arrays.fill(charSpaces, ' ');
                    System.out.print(new String(charSpaces));
                }
                System.out.println(VERSION_LABEL + " " + Version.VERSION);
            } catch (IOException e) {
                // DO NOTHING
            }
        }
    }

    private void serverStatus() {
        storageExecutor.status();
        serverExecutor.status();
    }

    private void clean(boolean overrideYes) {
        boolean storage = storageExecutor.isRunning();
        boolean server = serverExecutor.isRunning();
        if (storage || server) {
            System.out.println("Grakn is still running! Please do a shutdown with 'grakn server stop' before performing a cleanup.");
            return;
        }
        if (!overrideYes) {
            System.out.print("Are you sure you want to delete all stored data and logs? [y/N] ");
            System.out.flush();
            String response = new Scanner(System.in, StandardCharsets.UTF_8.name()).next();
            if (!response.equals("y") && !response.equals("Y")) {
                System.out.println("Response '" + response + "' did not equal 'y' or 'Y'.  Canceling clean operation.");
                return;
            }
        }
        storageExecutor.clean();
        serverExecutor.clean();
    }

    public static CommandLine buildCommand() {
        return GraknDaemonCommand.buildCommand("server");
    }

    public static CommandLine buildCommand(String name) {
        return GraknDaemonCommand.buildCommand(name);
    }

    @Command(
            name = "server",
            description = "Control Grakn server instances",
            mixinStandardHelpOptions = true,
            version = Version.VERSION,
            subcommands = {
                    GraknDaemonCommand.Start.class,
                    GraknDaemonCommand.Stop.class,
                    HelpCommand.class
            }
    )
    static class GraknDaemonCommand {

        // Disallow external command building, must obtain from .buildCommand()
        private GraknDaemonCommand() {
        }

        @Option(
                names = {"-l", "--no-logo"},
                negatable = true,
                description = "Toggle displaying the ASCII-art logo"
        )
        boolean showLogo = true;

        @Command(
                name = "start",
                description = "Start Grakn (or optionally, only one of the components)",
                synopsisSubcommandLabel = "[server | storage]",
                subcommands = HelpCommand.class
        )
        public static class Start implements Runnable {

            @ParentCommand
            GraknDaemonCommand parent;

            @Parameters(hidden = true)
            List<String> args = Collections.emptyList();

            @Override
            public void run() {
                GraknDaemon daemon = new GraknDaemon(parent.showLogo);
                daemon.storageExecutor.startIfNotRunning();
                daemon.serverExecutor.startIfNotRunning(args);
            }

            @Command(description = "Start the Grakn main server only")
            public void server(@Parameters(hidden = true) List<String> args) {
                GraknDaemon daemon = new GraknDaemon(parent.showLogo);
                daemon.serverExecutor.startIfNotRunning(args == null ? Collections.emptyList() : args);
            }

            @Command(description = "Stop the Grakn main server")
            public void storage() {
                GraknDaemon daemon = new GraknDaemon(parent.showLogo);
                daemon.storageExecutor.startIfNotRunning();
            }
        }

        @Command(
                name = "stop",
                description = "Stop Grakn (or optionally, only one of the components)",
                synopsisSubcommandLabel = "[server | storage]",
                subcommands = HelpCommand.class
        )
        public static class Stop implements Runnable {

            @ParentCommand
            GraknDaemonCommand parent;

            @Override
            public void run() {
                GraknDaemon daemon = new GraknDaemon(parent.showLogo);
                daemon.serverExecutor.stop();
                daemon.storageExecutor.stop();
            }

            @Command(description = "Stop the Grakn main server only")
            public void server() {
                GraknDaemon daemon = new GraknDaemon(parent.showLogo);
                daemon.serverExecutor.stop();
            }

            @Command(description = "Stop the Grakn Storage server only")
            public void storage() {
                GraknDaemon daemon = new GraknDaemon(parent.showLogo);
                daemon.storageExecutor.stop();
            }
        }

        @Command(description = "Check if Grakn is running")
        public void status() {
            GraknDaemon daemon = new GraknDaemon(showLogo);
            daemon.serverStatus();
        }

        @Command(description = "DANGEROUS: wipe data completely")
        public void clean(@Option(names = {"-y", "--yes"},
                description = "Override request for confirmation") boolean overrideYes) {
            GraknDaemon daemon = new GraknDaemon(showLogo);
            daemon.clean(overrideYes);
        }

        @Command(description = "Print version of Grakn server")
        public void version() {
            System.out.println(Version.VERSION);
        }

        public static CommandLine buildCommand(String name) {
            return new CommandLine(new GraknDaemonCommand())
                    .setCommandName(name);
        }
    }
}

