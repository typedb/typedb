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
import grakn.core.common.concurrent.ExecutorService;
import grakn.core.common.exception.GraknException;
import grakn.core.rocks.RocksGrakn;
import grakn.core.server.migrator.MigratorClient;
import grakn.core.server.rpc.GraknRPCService;
import grakn.core.server.rpc.MigratorRPCService;
import grakn.core.server.util.ServerCommand;
import grakn.core.server.util.ServerDefaults;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.PropertiesDefaultProvider;
import picocli.CommandLine.UnmatchedArgumentException;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static grakn.core.common.exception.ErrorMessage.Server.DATA_DIRECTORY_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.Server.DATA_DIRECTORY_NOT_WRITABLE;
import static grakn.core.common.exception.ErrorMessage.Server.ENV_VAR_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.Server.EXITED_WITH_ERROR;
import static grakn.core.common.exception.ErrorMessage.Server.FAILED_AT_STOPPING;
import static grakn.core.common.exception.ErrorMessage.Server.FAILED_PARSE_PROPERTIES;
import static grakn.core.common.exception.ErrorMessage.Server.PROPERTIES_FILE_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.Server.UNCAUGHT_EXCEPTION;
import static grakn.core.server.util.ServerDefaults.ASCII_LOGO_FILE;
import static grakn.core.server.util.ServerDefaults.PROPERTIES_FILE;


public class GraknServer implements AutoCloseable {

    static {
        java.util.logging.Logger.getLogger("io.grpc").setLevel(Level.SEVERE);
    }

    private static final Logger LOG = LoggerFactory.getLogger(GraknServer.class);
    private static final int MAX_THREADS = Runtime.getRuntime().availableProcessors();

    private final Grakn grakn;
    private final Server server;
    private final ServerCommand.Start command;
    private final GraknRPCService graknRPCService;
    private final MigratorRPCService migratorRPCService;

    private GraknServer(ServerCommand.Start command) throws IOException {
        this.command = command;
        configureAndVerifyDataDir();
        configureTracing();

        if (command.debug()) {
            LOG.info("Running Grakn Core Server in debug mode.");
        }

        grakn = RocksGrakn.open(command.dataDir());
        graknRPCService = new GraknRPCService(grakn);
        migratorRPCService = new MigratorRPCService(grakn);

        server = rpcServer();
        Runtime.getRuntime().addShutdownHook(NamedThreadFactory.create(GraknServer.class, "shutdown").newThread(this::close));
        Thread.setDefaultUncaughtExceptionHandler((Thread t, Throwable e) -> LOG.error(UNCAUGHT_EXCEPTION.message(t.getName()), e));
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
            final GrablTracing grablTracingClient;
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
        final Properties properties = new Properties();
        boolean error = false;

        try {
            properties.load(new FileInputStream(PROPERTIES_FILE));
        } catch (IOException e) {
            LOG.warn(PROPERTIES_FILE_NOT_FOUND.message(PROPERTIES_FILE.toString()));
            return new Properties();
        }

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            final String val = (String) entry.getValue();
            if (val.startsWith("$")) {
                final String envVarName = val.substring(1);
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
        final ServerCommand.Start startCommand = new ServerCommand.Start();
        final ServerCommand.ImportData importDataCommand = new ServerCommand.ImportData(startCommand);
        final ServerCommand.ExportData exportDataCommand = new ServerCommand.ExportData(startCommand);
        final ServerCommand.PrintSchema printSchemaCommand = new ServerCommand.PrintSchema(startCommand);
        final CommandLine commandLine = new CommandLine(startCommand)
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
            final ServerCommand command = parseCommandLine(parseProperties(), args);
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
            LOG.error(e.getMessage());
            LOG.error(EXITED_WITH_ERROR.message());
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
        final GraknServer server = new GraknServer(command);
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

    private Server rpcServer() {
        final NioEventLoopGroup workerELG = new NioEventLoopGroup(
                MAX_THREADS, NamedThreadFactory.create(GraknServer.class, "worker")
        );
        return NettyServerBuilder.forPort(command.port())
                .executor(ExecutorService.forkJoinPool())
                .workerEventLoopGroup(workerELG)
                .bossEventLoopGroup(workerELG)
                .maxConnectionIdle(1, TimeUnit.HOURS) // TODO: why 1 hour?
                .channelType(NioServerSocketChannel.class)
                .addService(graknRPCService)
                .addService(migratorRPCService)
                .build();
    }

    @Override
    public void close() {
        LOG.info("");
        LOG.info("Shutting down Grakn Core Server...");
        try {
            graknRPCService.close();
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
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw e;
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
