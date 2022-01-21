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

import ch.qos.logback.classic.LoggerContext;
import com.vaticle.factory.tracing.client.FactoryTracing;
import com.vaticle.factory.tracing.client.FactoryTracingThreadStatic;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import com.vaticle.typedb.common.util.Java;
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concurrent.executor.Executors;
import com.vaticle.typedb.core.migrator.MigratorClient;
import com.vaticle.typedb.core.migrator.MigratorService;
import com.vaticle.typedb.core.database.Factory;
import com.vaticle.typedb.core.database.CoreFactory;
import com.vaticle.typedb.core.server.option.Option;
import com.vaticle.typedb.core.server.option.cli.Command;
import com.vaticle.typedb.core.server.option.cli.CommandLine;
import com.vaticle.typedb.core.server.option.conf.Config;
import com.vaticle.typedb.core.server.option.conf.ConfigParser;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.ALREADY_RUNNING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.DATA_DIRECTORY_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.DATA_DIRECTORY_NOT_WRITABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.EXITED_WITH_ERROR;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.FAILED_AT_STOPPING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.INCOMPATIBLE_JAVA_RUNTIME;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNCAUGHT_EXCEPTION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNRECOGNISED_CLI_COMMAND;
import static com.vaticle.typedb.core.server.option.Option.configureLogback;
import static com.vaticle.typedb.core.server.common.Util.getTypedbDir;
import static com.vaticle.typedb.core.server.common.Util.printASCIILogo;

public class TypeDBServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TypeDBServer.class);

    protected final Factory factory;
    protected final TypeDB.DatabaseManager databaseMgr;
    protected final io.grpc.Server server;
    protected final Config config;
    protected final boolean debug;
    protected TypeDBService typeDBService;

    private TypeDBServer(Config config, boolean debug) {
        this(config, debug, new CoreFactory());
    }

    protected TypeDBServer(Config config, boolean debug, Factory factory) {
        this.config = config;
        this.debug = debug;

        configureAndVerifyJavaVersion();
        configureLogging();
        configureAndVerifyDataDir();
        configureTracing();

        if (debug) logger().info("Running {} in debug mode.", name());

        Options.Database options = new Options.Database()
                .typeDBDir(getTypedbDir())
                .dataDir(config.storage().dataDir())
                .storageDataCacheSize(config.storage().databaseCache().dataSize())
                .storageIndexCacheSize(config.storage().databaseCache().indexSize())
                .reasonerDebuggerDir(config.log().debugger().reasoner().output().path());

        this.factory = factory;
        databaseMgr = factory.databaseManager(options);
        server = rpcServer();
        Thread.setDefaultUncaughtExceptionHandler(
                (t, e) -> logger().error(UNCAUGHT_EXCEPTION.message(t.getName() + ": " + e.getMessage()), e)
        );
        Runtime.getRuntime().addShutdownHook(
                NamedThreadFactory.create(TypeDBServer.class, "shutdown").newThread(this::close)
        );
    }

    private void configureLogging() {
        configureLogback((LoggerContext) LoggerFactory.getILoggerFactory(), config);
        java.util.logging.Logger.getLogger("io.grpc").setLevel(Level.SEVERE);
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
        if (!Files.isDirectory(config.storage().dataDir())) {
            if (config.storage().dataDir().equals((new ConfigParser()).getConfig().storage().dataDir())) {
                try {
                    Files.createDirectory(config.storage().dataDir());
                } catch (IOException e) {
                    throw TypeDBException.of(e);
                }
            } else {
                throw TypeDBException.of(DATA_DIRECTORY_NOT_FOUND, config.storage().dataDir());
            }
        }

        if (!Files.isWritable(config.storage().dataDir())) {
            throw TypeDBException.of(DATA_DIRECTORY_NOT_WRITABLE, config.storage().dataDir());
        }
    }

    private void configureTracing() {
        if (config.vaticleFactory().enable()) {
            assert config.vaticleFactory().uri().isPresent() && config.vaticleFactory().username().isPresent() &&
                    config.vaticleFactory().token().isPresent();
            FactoryTracing factoryTracingClient;
            factoryTracingClient = FactoryTracing.create(
                    config.vaticleFactory().uri().get(),
                    config.vaticleFactory().username().get(),
                    config.vaticleFactory().token().get()
            ).withLogging();
            FactoryTracingThreadStatic.setGlobalTracingClient(factoryTracingClient);
            logger().info("Vaticle Factory tracing is enabled");
        }
    }

    protected io.grpc.Server rpcServer() {
        assert Executors.isInitialised();

        typeDBService = new TypeDBService(databaseMgr);
        MigratorService migratorService = new MigratorService(databaseMgr, Version.VERSION);

        return NettyServerBuilder.forAddress(config.server().address())
                .executor(Executors.service())
                .workerEventLoopGroup(Executors.network())
                .bossEventLoopGroup(Executors.network())
                .maxConnectionIdle(1, TimeUnit.HOURS) // TODO: why 1 hour?
                .channelType(NioServerSocketChannel.class)
                .addService(typeDBService)
                .addService(migratorService)
                .build();
    }

    protected String name() {
        return "TypeDB Server";
    }

    private InetSocketAddress address() {
        return config.server().address();
    }

    protected Path dataDir() {
        return config.storage().dataDir();
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
                throw TypeDBException.of(ALREADY_RUNNING, address());
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
            databaseMgr.close();
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

            CommandLine commandLine = Option.commandLine();
            Optional<Command> command = commandLine.parse(args);
            if (command.isEmpty()) {
                LOG.error(UNRECOGNISED_CLI_COMMAND.message(String.join(" ", args)));
                LOG.error(commandLine.usage());
                System.exit(1);
            } else if (command.get().isServer()) {
                if (command.get().asServer().isHelp()) System.out.println(commandLine.help());
                else if (command.get().asServer().isVersion()) System.out.println("Version: " + Version.VERSION);
                else runServer(command.get().asServer().configuration(), command.get().asServer().isDebug());
            } else if (command.get().isImport()) {
                importData(command.get().asImport());
            } else if (command.get().isExport()) {
                exportData(command.get().asExport());
            } else throw TypeDBException.of(ILLEGAL_STATE);
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

    private static void runServer(Config config, boolean debug) {
        Instant start = Instant.now();
        TypeDBServer server = new TypeDBServer(config, debug);
        server.start();
        Instant end = Instant.now();
        server.logger().info("- version: {}", Version.VERSION);
        server.logger().info("- listening to address: {}:{}", server.address().getHostString(), server.address().getPort());
        server.logger().info("- data directory configured to: {}", server.dataDir());
        server.logger().info("- bootup completed in: {} ms", Duration.between(start, end).toMillis());
        server.logger().info("...");
        server.serve();
    }

    protected static void exportData(Command.Export exportCommand) {
        MigratorClient migrator = new MigratorClient(exportCommand.typedbPort());
        boolean success = migrator.exportData(exportCommand.database(), exportCommand.file());
        System.exit(success ? 0 : 1);
    }

    protected static void importData(Command.Import importCommand) {
        MigratorClient migrator = new MigratorClient(importCommand.typedbPort());
        boolean success = migrator.importData(importCommand.database(), importCommand.file());
        System.exit(success ? 0 : 1);
    }
}
