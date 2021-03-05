/*
 * Copyright (C) 2021 Grakn Labs
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
 *
 */

package grakn.core.server;

import grabl.tracing.client.GrablTracing;
import grabl.tracing.client.GrablTracingThreadStatic;
import grakn.common.concurrent.NamedThreadFactory;
import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Options;
import grakn.core.concurrent.common.Executors;
import grakn.core.migrator.MigratorClient;
import grakn.core.rocks.RocksGrakn;
import grakn.core.server.common.ServerCommand;
import grakn.core.server.common.ServerDefaults;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.PropertiesDefaultProvider;
import picocli.CommandLine.UnmatchedArgumentException;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.BindException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static grakn.core.common.exception.ErrorMessage.Server.ALREADY_RUNNING;
import static grakn.core.common.exception.ErrorMessage.Server.DATA_DIRECTORY_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.Server.DATA_DIRECTORY_NOT_WRITABLE;
import static grakn.core.common.exception.ErrorMessage.Server.ENV_VAR_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.Server.EXITED_WITH_ERROR;
import static grakn.core.common.exception.ErrorMessage.Server.FAILED_AT_STOPPING;
import static grakn.core.common.exception.ErrorMessage.Server.FAILED_PARSE_PROPERTIES;
import static grakn.core.common.exception.ErrorMessage.Server.PROPERTIES_FILE_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.Server.UNCAUGHT_EXCEPTION;
import static grakn.core.server.common.ServerDefaults.ASCII_LOGO_FILE;
import static grakn.core.server.common.ServerDefaults.PROPERTIES_FILE;


public class GraknServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(GraknServer.class);

    private final Grakn grakn;
    private final Server server;
    private final ServerCommand.Start command;
    private final GraknService graknService;
    private final MigratorService migratorRPCService;

    private GraknServer(ServerCommand.Start command) throws IOException {
        this.command = command;
        configureAndVerifyDataDir();
        configureTracing();

        if (command.debug()) LOG.info("Running Grakn Core Server in debug mode.");

        Options.Database options = new Options.Database()
                .graknDir(ServerDefaults.GRAKN_DIR)
                .dataDir(command.dataDir())
                .logsDir(command.logsDir());
        grakn = RocksGrakn.open(options);
        graknService = new GraknService(grakn);
        migratorRPCService = new MigratorService(grakn);

        server = rpcServer();
        Thread.setDefaultUncaughtExceptionHandler(
                (t, e) -> LOG.error(UNCAUGHT_EXCEPTION.message(t.getName() + ": " + e.getMessage()), e)
        );
        Runtime.getRuntime().addShutdownHook(
                NamedThreadFactory.create(GraknServer.class, "shutdown").newThread(this::close)
        );

        initLoggerConfig();
    }

    private Server rpcServer() {
        assert Executors.isInitialised();
        return NettyServerBuilder.forPort(command.port())
                .executor(Executors.service())
                .workerEventLoopGroup(Executors.network())
                .bossEventLoopGroup(Executors.network())
                .maxConnectionIdle(1, TimeUnit.HOURS) // TODO: why 1 hour?
                .channelType(NioServerSocketChannel.class)
                .addService(graknService)
                .addService(migratorRPCService)
                .build();
    }

    private void initLoggerConfig() {
        java.util.logging.Logger.getLogger("io.grpc").setLevel(Level.SEVERE);
    }

    private int port() {
        return command.port();
    }

    private Path dataDir() {
        return command.dataDir();
    }

    private void configureAndVerifyDataDir() throws IOException {
        if (!Files.isDirectory(this.command.dataDir())) {
            if (this.command.dataDir().equals(ServerDefaults.DATA_DIR)) {
                Files.createDirectory(this.command.dataDir());
            } else {
                throw GraknException.of(DATA_DIRECTORY_NOT_FOUND, this.command.dataDir());
            }
        }

        if (!Files.isWritable(this.command.dataDir())) {
            throw GraknException.of(DATA_DIRECTORY_NOT_WRITABLE, this.command.dataDir());
        }
    }

    private void configureTracing() {
        if (this.command.grablTrace()) {
            GrablTracing grablTracingClient;
            grablTracingClient = GrablTracing.withLogging(GrablTracing.tracing(
                    command.grablURI().toString(),
                    command.grablUsername(),
                    command.grablToken()
            ));
            GrablTracingThreadStatic.setGlobalTracingClient(grablTracingClient);
            LOG.info("Grabl tracing is enabled");
        }
    }

    private static void printASCIILogo() throws IOException {
        if (ASCII_LOGO_FILE.exists()) {
            System.out.println("\n" + new String(Files.readAllBytes(ASCII_LOGO_FILE.toPath()), StandardCharsets.UTF_8));
        }
    }

    private static Properties parseProperties() {
        Properties properties = new Properties();
        boolean error = false;

        try {
            properties.load(new FileInputStream(PROPERTIES_FILE));
        } catch (IOException e) {
            LOG.warn(PROPERTIES_FILE_NOT_FOUND.message(PROPERTIES_FILE.toString()));
            return new Properties();
        }

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String val = (String) entry.getValue();
            if (val.startsWith("$")) {
                String envVarName = val.substring(1);
                if (System.getenv(envVarName) == null) {
                    LOG.error(ENV_VAR_NOT_FOUND.message(val));
                    error = true;
                } else {
                    properties.put(entry.getKey(), System.getenv(envVarName));
                }
            }
        }

        if (error) throw GraknException.of(FAILED_PARSE_PROPERTIES);
        else return properties;
    }

    private static ServerCommand parseCommandLine(Properties properties, String[] args) {
        ServerCommand.Start startCommand = new ServerCommand.Start();
        ServerCommand.ImportData importDataCommand = new ServerCommand.ImportData(startCommand);
        ServerCommand.ExportData exportDataCommand = new ServerCommand.ExportData(startCommand);
        ServerCommand.PrintSchema printSchemaCommand = new ServerCommand.PrintSchema(startCommand);
        CommandLine commandLine = new CommandLine(startCommand)
                .addSubcommand(importDataCommand)
                .addSubcommand(exportDataCommand)
                .addSubcommand(printSchemaCommand);
        commandLine.setDefaultValueProvider(new PropertiesDefaultProvider(properties));

        try {
            CommandLine.ParseResult parseResult = commandLine.parseArgs(args);
            if (commandLine.isUsageHelpRequested()) {
                commandLine.usage(commandLine.getOut());
                return null;
            } else if (commandLine.isVersionHelpRequested()) {
                commandLine.printVersionHelp(commandLine.getOut());
                return null;
            } else {
                if (parseResult.hasSubcommand()) {
                    assert parseResult.subcommand().asCommandLineList().size() == 1;
                    return parseResult.subcommand().asCommandLineList().get(0).getCommand();
                } else {
                    assert parseResult.asCommandLineList().size() == 1;
                    return parseResult.asCommandLineList().get(0).getCommand();
                }
            }
        } catch (ParameterException ex) {
            commandLine.getErr().println(ex.getMessage());
            if (!UnmatchedArgumentException.printSuggestions(ex, commandLine.getErr())) {
                ex.getCommandLine().usage(commandLine.getErr());
            }
            return null;
        }
    }

    public static void main(String[] args) {
        try {
            printASCIILogo();
            ServerCommand command = parseCommandLine(parseProperties(), args);
            if (command == null) System.exit(0);

            if (command.isStart()) {
                startGraknServer(command.asStart());
            } else if (command.isImportData()) {
                importData(command.asImportData());
            } else if (command.isExportData()) {
                exportData(command.asExportData());
            } else if (command.isPrintSchema()) {
                ServerCommand.PrintSchema printSchemaCommand = command.asPrintSchema();
                printSchema(printSchemaCommand);
            }
        } catch (Exception e) {
            if (e instanceof GraknException) {
                LOG.error(e.getMessage());
            } else {
                LOG.error(e.getMessage(), e);
                LOG.error(EXITED_WITH_ERROR.message());
            }
            System.exit(1);
        }

        System.exit(0);
    }

    private static void printSchema(ServerCommand.PrintSchema printSchemaCommand) {
        MigratorClient migrator = new MigratorClient(printSchemaCommand.port());
        migrator.printSchema(printSchemaCommand.database());
    }

    private static void exportData(ServerCommand.ExportData exportDataCommand) {
        MigratorClient migrator = new MigratorClient(exportDataCommand.port());
        boolean success = migrator.exportData(exportDataCommand.database(), exportDataCommand.filename());
        System.exit(success ? 0 : 1);
    }

    private static void importData(ServerCommand.ImportData importDataCommand) {
        MigratorClient migrator = new MigratorClient(importDataCommand.port());
        boolean success = migrator.importData(importDataCommand.database(), importDataCommand.filename(), importDataCommand.remapLabels());
        System.exit(success ? 0 : 1);
    }

    private static void startGraknServer(ServerCommand.Start command) throws IOException {
        Instant start = Instant.now();
        GraknServer server = new GraknServer(command);
        server.start();
        Instant end = Instant.now();
        LOG.info("- version: {}", Version.VERSION);
        LOG.info("- listening to port: {}", server.port());
        LOG.info("- data directory configured to: {}", server.dataDir());
        LOG.info("- bootup completed in: {} ms", Duration.between(start, end).toMillis());
        LOG.info("");
        LOG.info("Grakn Core Server is now running and will keep this process alive.");
        LOG.info("You can press CTRL+C to shutdown this server.");
        LOG.info("...");
        server.serve();
    }

    @Override
    public void close() {
        LOG.info("");
        LOG.info("Shutting down Grakn Core Server...");
        try {
            graknService.close();
            server.shutdown();
            server.awaitTermination();
            grakn.close();
            System.runFinalization();
            LOG.info("Grakn Core Server has been shutdown");
        } catch (InterruptedException e) {
            LOG.error(FAILED_AT_STOPPING.message(), e);
            Thread.currentThread().interrupt();
        }
    }

    private void start() throws IOException {
        try {
            server.start();
        } catch (IOException e) {
            if (e.getCause() != null && e.getCause() instanceof BindException) {
                throw GraknException.of(ALREADY_RUNNING, port());
            } else {
                throw e;
            }
        }
    }

    private void serve() {
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            // server is terminated
            close();
            Thread.currentThread().interrupt();
        }
    }
}
