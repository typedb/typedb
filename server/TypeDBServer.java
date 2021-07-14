/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.server;

import com.vaticle.factory.tracing.client.FactoryTracing;
import com.vaticle.factory.tracing.client.FactoryTracingThreadStatic;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import com.vaticle.typedb.common.util.Java;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concurrent.executor.Executors;
import com.vaticle.typedb.core.migrator.MigratorClient;
import com.vaticle.typedb.core.migrator.MigratorService;
import com.vaticle.typedb.core.rocks.Factory;
import com.vaticle.typedb.core.rocks.RocksFactory;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
import com.vaticle.typedb.core.server.common.RunOptions;
import com.vaticle.typedb.core.server.common.ServerDefaults;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.ALREADY_RUNNING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.DATA_DIRECTORY_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.DATA_DIRECTORY_NOT_WRITABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.EXITED_WITH_ERROR;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.FAILED_AT_STOPPING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.INCOMPATIBLE_JAVA_RUNTIME;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNCAUGHT_EXCEPTION;
import static com.vaticle.typedb.core.server.common.Util.parseCommandLine;
import static com.vaticle.typedb.core.server.common.Util.parseProperties;
import static com.vaticle.typedb.core.server.common.Util.printASCIILogo;


public class TypeDBServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TypeDBServer.class);

    protected final Factory factory;
    protected final RocksTypeDB typedb;
    protected final io.grpc.Server server;
    protected final RunOptions.Server command;
    protected TypeDBService typeDBService;

    private TypeDBServer(RunOptions.Server command) {
        this(command, new RocksFactory());
    }

    protected TypeDBServer(RunOptions.Server command, Factory factory) {
        this.command = command;
        configureAndVerifyJavaVersion();
        configureAndVerifyDataDir();
        configureTracing();

        if (command.debug()) logger().info("Running {} in debug mode.", name());

        Options.Database options = new Options.Database()
                .typeDBDir(ServerDefaults.TYPEDB_DIR)
                .dataDir(command.dataDir())
                .logsDir(command.logsDir());
        this.factory = factory;
        typedb = factory.typedb(options);
        server = rpcServer();
        Thread.setDefaultUncaughtExceptionHandler(
                (t, e) -> logger().error(UNCAUGHT_EXCEPTION.message(t.getName() + ": " + e.getMessage()), e)
        );
        Runtime.getRuntime().addShutdownHook(
                NamedThreadFactory.create(TypeDBServer.class, "shutdown").newThread(this::close)
        );

        initLoggerConfig();
    }

    private void configureAndVerifyJavaVersion() {
        int majorVersion = Java.getMajorVersion();
        if (majorVersion == Java.UNKNOWN_VERSION) {
            logger().warn("Could not detect Java version from version string '{}'. Will start {} anyway.", System.getProperty("java.version"), name());
        } else if (majorVersion < 11) {
            throw TypeDBException.of(INCOMPATIBLE_JAVA_RUNTIME, majorVersion);
        }
    }

    private void configureAndVerifyDataDir() {
        if (!Files.isDirectory(this.command.dataDir())) {
            if (this.command.dataDir().equals(ServerDefaults.DATA_DIR)) {
                try {
                    Files.createDirectory(this.command.dataDir());
                } catch (IOException e) {
                    throw TypeDBException.of(e);
                }
            } else {
                throw TypeDBException.of(DATA_DIRECTORY_NOT_FOUND, this.command.dataDir());
            }
        }

        if (!Files.isWritable(this.command.dataDir())) {
            throw TypeDBException.of(DATA_DIRECTORY_NOT_WRITABLE, this.command.dataDir());
        }
    }

    private void configureTracing() {
        if (this.command.factoryTrace()) {
            FactoryTracing factoryTracingClient;
            factoryTracingClient = FactoryTracing.create(
                    command.factoryURI().toString(),
                    command.factoryUsername(),
                    command.factoryToken()
            ).withLogging();
            FactoryTracingThreadStatic.setGlobalTracingClient(factoryTracingClient);
            logger().info("Vaticle Factory tracing is enabled");
        }
    }

    protected io.grpc.Server rpcServer() {
        assert Executors.isInitialised();

        typeDBService = new TypeDBService(typedb);
        MigratorService migratorService = new MigratorService(typedb, Version.VERSION);

        return NettyServerBuilder.forPort(command.port())
                .executor(Executors.service())
                .workerEventLoopGroup(Executors.network())
                .bossEventLoopGroup(Executors.network())
                .maxConnectionIdle(1, TimeUnit.HOURS) // TODO: why 1 hour?
                .channelType(NioServerSocketChannel.class)
                .addService(typeDBService)
                .addService(migratorService)
                .build();
    }

    private void initLoggerConfig() {
        java.util.logging.Logger.getLogger("io.grpc").setLevel(Level.SEVERE);
    }

    protected String name() {
        return "TypeDB Server";
    }

    private int port() {
        return command.port();
    }

    protected Path dataDir() {
        return command.dataDir();
    }

    protected Logger logger() {
        return LOG;
    }

    protected void start() {
        try {
            server.start();
            logger().info("{} is now running and will keep this process alive.", name());
            logger().info("You can press CTRL+C to shutdown this server.");
            logger().info("");
        } catch (IOException e) {
            if (e.getCause() != null && e.getCause() instanceof BindException) {
                throw TypeDBException.of(ALREADY_RUNNING, port());
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    protected void serve() {
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            // server is terminated
            close();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        logger().info("");
        logger().info("Shutting down {}...", name());
        try {
            assert typeDBService != null;
            typeDBService.close();
            server.shutdown();
            server.awaitTermination();
            typedb.close();
            System.runFinalization();
            logger().info("{} has been shutdown", name());
        } catch (InterruptedException e) {
            logger().error(FAILED_AT_STOPPING.message(), e);
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        try {
            printASCIILogo();
            Optional<RunOptions> options = parseCommandLine(parseProperties(), args);
            if (options.isEmpty()) System.exit(0);
            else if (options.get().isServer()) runServer(options.get().asServer());
            else if (options.get().isDataImport()) importData(options.get().asDataImport());
            else if (options.get().isDataExport()) exportData(options.get().asDataExport());
            else assert false;
        } catch (Exception e) {
            if (e instanceof TypeDBException) {
                LOG.error(e.getMessage());
            } else {
                LOG.error(e.getMessage(), e);
                LOG.error(EXITED_WITH_ERROR.message());
            }
            System.exit(1);
        }

        System.exit(0);
    }

    private static void runServer(RunOptions.Server command) {
        Instant start = Instant.now();
        TypeDBServer server = new TypeDBServer(command);
        server.start();
        Instant end = Instant.now();
        LOG.info("- version: {}", Version.VERSION);
        LOG.info("- listening to port: {}", server.port());
        LOG.info("- data directory configured to: {}", server.dataDir());
        LOG.info("- bootup completed in: {} ms", Duration.between(start, end).toMillis());
        LOG.info("...");
        server.serve();
    }

    protected static void exportData(RunOptions.DataExport exportDataCommand) {
        MigratorClient migrator = new MigratorClient(exportDataCommand.port());
        boolean success = migrator.exportData(exportDataCommand.database(), exportDataCommand.filename());
        System.exit(success ? 0 : 1);
    }

    protected static void importData(RunOptions.DataImport importDataCommand) {
        MigratorClient migrator = new MigratorClient(importDataCommand.port());
        boolean success = migrator.importData(importDataCommand.database(), importDataCommand.filename(), importDataCommand.remapLabels());
        System.exit(success ? 0 : 1);
    }
}
