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
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concurrent.executor.Executors;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreFactory;
import com.vaticle.typedb.core.database.Factory;
import com.vaticle.typedb.core.migrator.MigratorClient;
import com.vaticle.typedb.core.migrator.MigratorService;
import com.vaticle.typedb.core.server.logging.CoreLogback;
import com.vaticle.typedb.core.server.parameters.CoreConfig;
import com.vaticle.typedb.core.server.parameters.CoreConfigParser;
import com.vaticle.typedb.core.server.parameters.ServerSubcommand;
import com.vaticle.typedb.core.server.parameters.ServerSubcommandParser;
import com.vaticle.typedb.core.server.parameters.util.ArgsParser;
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
import static com.vaticle.typedb.core.server.common.Util.getTypedbDir;
import static com.vaticle.typedb.core.server.common.Util.printASCIILogo;

public class TypeDBServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TypeDBServer.class);

    protected final Factory factory;
    protected final CoreDatabaseManager databaseMgr;
    protected final io.grpc.Server server;
    protected final CoreConfig config;
    protected final boolean debug;
    protected TypeDBService typeDBService;
    protected final CoreLogback logback;

    private TypeDBServer(CoreConfig config, boolean debug) {
        this(config, debug, new CoreFactory(), new CoreLogback());
    }

    protected TypeDBServer(CoreConfig config, boolean debug, Factory factory, CoreLogback logback) {
        this.config = config;
        this.debug = debug;
        this.logback = logback;

        verifyJavaVersion();
        verifyDataDir();
        configureLogging();
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

    private void verifyJavaVersion() {
        int majorVersion = Java.getMajorVersion();
        if (majorVersion == Java.UNKNOWN_VERSION) {
            logger().warn("Could not detect Java version from version string '{}'. Will start {} anyway.", System.getProperty("java.version"), name());
        } else if (majorVersion < 11) {
            throw TypeDBException.of(INCOMPATIBLE_JAVA_RUNTIME, majorVersion);
        }
    }

    private void verifyDataDir() {
        if (!Files.isDirectory(config.storage().dataDir())) {
            throw TypeDBException.of(DATA_DIRECTORY_NOT_FOUND, this.config.storage().dataDir());
        }

        if (!Files.isWritable(config.storage().dataDir())) {
            throw TypeDBException.of(DATA_DIRECTORY_NOT_WRITABLE, config.storage().dataDir());
        }
    }

    private void configureLogging() {
        logback.configure((LoggerContext) LoggerFactory.getILoggerFactory(), config.log());
        java.util.logging.Logger.getLogger("io.grpc").setLevel(Level.SEVERE);
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

            CoreConfigParser configParser = new CoreConfigParser();
            ArgsParser<ServerSubcommand<CoreConfig>> argsParser = new ArgsParser<ServerSubcommand<CoreConfig>>()
                    .subcommand(new ServerSubcommandParser.Server<>(configParser))
                    .subcommand(new ServerSubcommandParser.Import<>())
                    .subcommand(new ServerSubcommandParser.Export<>());
            Optional<ServerSubcommand<CoreConfig>> subcmd = argsParser.parse(args);
            if (subcmd.isEmpty()) {
                LOG.error(UNRECOGNISED_CLI_COMMAND.message(String.join(" ", args)));
                LOG.error(argsParser.usage());
                System.exit(1);
            } else {
                if (subcmd.get().isServer()) {
                    ServerSubcommand.Server<CoreConfig> subcmdServer = subcmd.get().asServer();
                    if (subcmdServer.isHelp()) System.out.println(argsParser.help());
                    else if (subcmdServer.isVersion()) System.out.println("Version: " + Version.VERSION);
                    else runServer(subcmdServer);
                } else if (subcmd.get().isImport()) {
                    importData(subcmd.get().asImport());
                } else if (subcmd.get().isExport()) {
                    exportData(subcmd.get().asExport());
                } else throw TypeDBException.of(ILLEGAL_STATE);
            }
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

    private static void runServer(ServerSubcommand.Server<CoreConfig> subcmdServer) {
        Instant start = Instant.now();
        TypeDBServer server = new TypeDBServer(subcmdServer.config(), subcmdServer.isDebug());
        server.start();
        Instant end = Instant.now();
        server.logger().info("- version: {}", Version.VERSION);
        server.logger().info("- listening to address: {}:{}", server.address().getHostString(), server.address().getPort());
        server.logger().info("- data directory configured to: {}", server.dataDir());
        server.logger().info("- bootup completed in: {} ms", Duration.between(start, end).toMillis());
        server.logger().info("...");
        server.serve();
    }

    protected static void exportData(ServerSubcommand.Export<?> subcmdExport) {
        MigratorClient migrator = new MigratorClient(subcmdExport.port());
        boolean success = migrator.exportData(subcmdExport.database(), subcmdExport.file());
        System.exit(success ? 0 : 1);
    }

    protected static void importData(ServerSubcommand.Import<?> subcmdImport) {
        MigratorClient migrator = new MigratorClient(subcmdImport.port());
        boolean success = migrator.importData(subcmdImport.database(), subcmdImport.file());
        System.exit(success ? 0 : 1);
    }
}
