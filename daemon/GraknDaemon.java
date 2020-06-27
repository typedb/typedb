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

import grakn.core.common.config.Config;
import grakn.core.common.config.ConfigKey;
import grakn.core.common.config.SystemProperty;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.daemon.executor.Executor;
import grakn.core.daemon.executor.Server;
import grakn.core.daemon.executor.Storage;
import grakn.core.server.Version;
import grakn.core.server.migrate.proto.MigrateProto;
import grakn.core.server.migrate.proto.MigrateServiceGrpc;
import grakn.core.server.rpc.MigrateService;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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

    private static final String SERVER = "server";
    private static final String STORAGE = "storage";
    private static final String EMPTY_STRING = "";
    private static final String OPTIONS = "--";
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
        try {
            Path graknHome = Paths.get(Objects.requireNonNull(SystemProperty.CURRENT_DIRECTORY.value()));
            Path graknProperties = Paths.get(Objects.requireNonNull(SystemProperty.CONFIGURATION_FILE.value()));
            assertEnvironment(graknHome, graknProperties);

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
                int spaces = lineLength - (VERSION_LABEL.length() + Version.VERSION.length());

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

    /**
     * Accepts various Grakn commands (eg., 'grakn server start')
     *
     * @param args arrays of arguments, eg., { 'server', 'start' }
     */
    public void run(String[] args) {
//        String action = args.length > 1 ? args[1] : "";
//        String option = args.length > 2 ? args[2] : "";

//        List<String> mainArgs = new ArrayList<>();
//        List<String> otherArgs = Collections.emptyList();

        // Ignore command name arg by starting at one

        int splitPoint = 1;
        while (splitPoint < args.length && !OPTIONS.equals(args[splitPoint])) {
            splitPoint++;
        }

        List<String> mainArgs = Arrays.asList(args).subList(1, splitPoint);
        List<String> otherArgs = splitPoint < args.length
                ? Arrays.asList(args).subList(splitPoint + 1, args.length)
                : Collections.emptyList();

        String action = mainArgs.size() > 0 ? mainArgs.get(0) : "";
        String option = mainArgs.size() > 1 ? mainArgs.get(1) : "";

        switch (action) {
            case "start":
                printGraknLogo();
                serverStart(option, otherArgs);
                break;
            case "stop":
                printGraknLogo();
                serverStop(option);
                break;
            case "status":
                printGraknLogo();
                serverStatus();
                break;
            case "clean":
                clean();
                break;
            case "version":
                version();
                break;
            case "export":
                export(mainArgs.subList(1, mainArgs.size()));
                break;
            case "import":
                import_(mainArgs.subList(1, mainArgs.size()));
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
            case EMPTY_STRING:
                serverExecutor.stop();
                storageExecutor.stop();
                break;
            default:
                serverHelp();
        }
    }

    private void serverStart(String arg, List<String> otherArgs) {
        switch (arg) {
            case SERVER:
                serverExecutor.startIfNotRunning(otherArgs);
                break;
            case STORAGE:
                storageExecutor.startIfNotRunning();
                break;
            case OPTIONS:
            case EMPTY_STRING:
                storageExecutor.startIfNotRunning();
                serverExecutor.startIfNotRunning(otherArgs);
                break;
            default:
                serverHelp();
        }
    }

    private void serverHelp() {
        System.out.println("Usage: grakn server COMMAND\n" +
                                   "\n" +
                                   "COMMAND:\n" +
                                   "start [" + SERVER + "|" + STORAGE + "] Start Grakn (or optionally, only one of the component)\n" +
                                   "stop [" + SERVER + "|" + STORAGE + "]   Stop Grakn (or optionally, only one of the component)\n" +
                                   "status                         Check if Grakn is running\n" +
                                   "clean                          DANGEROUS: wipe data completely\n" +
                                   "version                        Print version of Grakn server\n" +
                                   "\n" +
                                   "Tips:\n" +
                                   "- Start Grakn with 'grakn server start'\n" +
                                   "- Start or stop only one component with, e.g. 'grakn server start storage' or 'grakn server stop storage', respectively\n");
    }

    private void serverStatus() {
        storageExecutor.status();
        serverExecutor.status();
    }

    private void version() {
        System.out.println(Version.VERSION);
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

    private void export(List<String> args) {
        String keyspace = args.get(0);
        String path = Paths.get(args.get(1)).toAbsolutePath().toString();
        MigrateServiceGrpc.MigrateServiceBlockingStub stub = connectLocal();
        MigrateProto.ExportFile.Res res = stub.exportFile(MigrateProto.ExportFile.Req.newBuilder().setName(keyspace).setPath(path).build());
    }

    private void import_(List<String> args) {
        String keyspace = args.get(0);
        String path = Paths.get(args.get(1)).toAbsolutePath().toString();
        MigrateServiceGrpc.MigrateServiceBlockingStub stub = connectLocal();
        MigrateProto.ImportFile.Res res = stub.importFile(MigrateProto.ImportFile.Req.newBuilder().setName(keyspace).setPath(path).build());
    }

    private MigrateServiceGrpc.MigrateServiceBlockingStub connectLocal() {
        ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:" + readPort())
                .usePlaintext().build();
        return MigrateServiceGrpc.newBlockingStub(channel);
    }

    private int readPort() {
        return Config.read(Paths.get(Objects.requireNonNull(SystemProperty.CONFIGURATION_FILE.value()))).getProperty(ConfigKey.GRPC_PORT);
    }
}

