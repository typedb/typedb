/*
 * Copyright (C) 2022 Vaticle
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
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import com.vaticle.typedb.common.util.Java;
import com.vaticle.typedb.core.common.diagnostics.CoreErrorReporter;
import com.vaticle.typedb.core.common.diagnostics.Diagnostics;
import com.vaticle.typedb.core.common.exception.TypeDBCheckedException;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concurrent.executor.Executors;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreFactory;
import com.vaticle.typedb.core.database.Factory;
import com.vaticle.typedb.core.migrator.CoreMigratorClient;
import com.vaticle.typedb.core.migrator.MigratorService;
import com.vaticle.typedb.core.server.common.Constants;
import com.vaticle.typedb.core.server.logging.CoreLogback;
import com.vaticle.typedb.core.server.parameters.CoreConfig;
import com.vaticle.typedb.core.server.parameters.CoreConfigParser;
import com.vaticle.typedb.core.server.parameters.CoreSubcommand;
import com.vaticle.typedb.core.server.parameters.CoreSubcommandParser;
import com.vaticle.typedb.core.server.parameters.util.ArgsParser;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.sentry.Sentry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.JAVA_ERROR;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNKNOWN_ERROR;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.DATA_DIRECTORY_NOT_WRITABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.FAILED_AT_STOPPING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.FAILED_TO_CREATE_DATA_DIRECTORY;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.INCOMPATIBLE_JAVA_RUNTIME;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.PORT_IN_USE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNCAUGHT_ERROR;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNRECOGNISED_CLI_COMMAND;
import static com.vaticle.typedb.core.server.common.Constants.SERVER_ID_ALPHABET;
import static com.vaticle.typedb.core.server.common.Constants.SERVER_ID_FILE_NAME;
import static com.vaticle.typedb.core.server.common.Constants.SERVER_ID_LENGTH;
import static com.vaticle.typedb.core.server.common.Constants.TYPEDB_DISTRIBUTION_NAME;
import static com.vaticle.typedb.core.server.common.Util.getTypedbDir;
import static com.vaticle.typedb.core.server.common.Util.printASCIILogo;
import static org.slf4j.Logger.ROOT_LOGGER_NAME;

public class TypeDBServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TypeDBServer.class);

    protected final Factory factory;
    protected final CoreDatabaseManager databaseMgr;
    protected final io.grpc.Server server;
    protected final boolean debug;
    protected TypeDBService typeDBService;
    protected AtomicBoolean isOpen;
    private final CoreConfig config;
    private final CoreLogback logback;

    static TypeDBServer create(CoreConfig config, boolean debug) {
        CoreLogback logback = new CoreLogback();
        configureLogging(logback, config);
        return new TypeDBServer(config, logback, debug, new CoreFactory());
    }

    protected static void configureLogging(CoreLogback logback, CoreConfig config) {
        logback.configure((LoggerContext) LoggerFactory.getILoggerFactory(), config.log());
        java.util.logging.Logger.getLogger("io.grpc").setLevel(Level.SEVERE);
    }

    protected TypeDBServer(CoreConfig config, CoreLogback logback, boolean debug, Factory factory) {
        this.config = config;
        this.logback = logback;
        this.debug = debug;

        verifyJavaVersion();
        verifyPort();
        createOrVerifyDataDir();
        configureDiagnostics();

        if (debug) logger().info("Running {} in debug mode.", name());

        Options.Database options = new Options.Database()
                .typeDBDir(getTypedbDir())
                .dataDir(config.storage().dataDir())
                .storageDataCacheSize(config.storage().databaseCache().dataSize())
                .storageIndexCacheSize(config.storage().databaseCache().indexSize())
                .reasonerDebuggerDir(config.log().debugger().reasonerTracer().output().baseDirectory())
                .reasonerPerfCounters(config.log().debugger().reasonerPerfCounters().isEnabled());

        this.factory = factory;
        databaseMgr = factory.databaseManager(options);
        server = rpcServer();
        Thread.setDefaultUncaughtExceptionHandler(
                (t, e) -> {
                    try {
                        logger().error(UNCAUGHT_ERROR.message(t.getName(), e), e);
                        close();
                        Sentry.captureException(e);
                        System.exit(1);
                    } catch (TypeDBCheckedException ex) {
                        logger().error("Failed to shut down cleanly, performing hard stop.");
                        Runtime.getRuntime().halt(1);
                    } catch (Throwable error) {
                        // another thread will do the close
                    }
                }
        );
        Runtime.getRuntime().addShutdownHook(
                NamedThreadFactory.create(TypeDBServer.class, "shutdown").newThread(() -> {
                    try {
                        close();
                    } catch (Throwable error) {
                        logger().error("Error during shutdown: ", error);
                    }
                })
        );
        isOpen = new AtomicBoolean(true);
    }

    private void verifyJavaVersion() {
        int majorVersion = Java.getMajorVersion();
        if (majorVersion == Java.UNKNOWN_VERSION) {
            logger().warn("Could not detect Java version from version string '{}'. Will start {} anyway.", System.getProperty("java.version"), name());
        } else if (majorVersion < 11) {
            logger().error(INCOMPATIBLE_JAVA_RUNTIME.message(majorVersion));
            System.exit(1);
        }
    }

    private void verifyPort() {
        int port = config.server().address().getPort();
        try (ServerSocket serverSocket = new ServerSocket()) {
            // setReuseAddress(false) is required only on macOS,
            // otherwise the code will not work correctly on that platform
            serverSocket.setReuseAddress(false);
            serverSocket.bind(new InetSocketAddress(InetAddress.getByName("localhost"), port), 1);
        } catch (Exception ex) {
            logger().error(PORT_IN_USE.message(address()));
            System.exit(1);
        }
    }

    private void createOrVerifyDataDir() {
        if (!Files.isDirectory(config.storage().dataDir())) {
            try {
                Path path = Files.createDirectories(config.storage().dataDir());
                if (!path.toFile().setWritable(true)) {
                    throw TypeDBException.of(DATA_DIRECTORY_NOT_WRITABLE, path);
                }
            } catch (IOException e) {
                throw TypeDBException.of(FAILED_TO_CREATE_DATA_DIRECTORY, this.config.storage().dataDir(), e);
            }
        } else if (!Files.isWritable(config.storage().dataDir())) {
            throw TypeDBException.of(DATA_DIRECTORY_NOT_WRITABLE, config.storage().dataDir());
        }
    }

    protected void configureDiagnostics() {
        try {
            Diagnostics.initialise(
                    config.diagnostics().reporting().enable(), serverID(), name(), Version.VERSION,
                    Constants.DIAGNOSTICS_REPORTING_URI, new CoreErrorReporter()
            );
        } catch (Throwable e) {
            LOG.debug("Failed to initialise diagnostics: ", e);
        }
    }

    protected String serverID() {
        try {
            return serverIDStored();
        } catch (Exception e) {
            LOG.debug("Failed to create, persist, or read stored server ID: ", e);
        }
        // fallback
        return "_0";
    }

    private String serverIDStored() throws IOException {
        Path serverIDFile = config().storage().dataDir().resolve(SERVER_ID_FILE_NAME);
        if (serverIDFile.toFile().exists()) {
            return Files.readString(serverIDFile);
        } else {
            Random random = new Random();
            String serverID = IntStream.range(0, SERVER_ID_LENGTH).boxed()
                    .map(i -> SERVER_ID_ALPHABET.charAt(random.nextInt(SERVER_ID_ALPHABET.length())))
                    .map(String::valueOf)
                    .collect(Collectors.joining());
            Files.writeString(serverIDFile, serverID);
            return serverID;
        }
    }

    protected io.grpc.Server rpcServer() {
        assert Executors.isInitialised();

        typeDBService = new TypeDBService(config.server().address(), databaseMgr);
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
        return TYPEDB_DISTRIBUTION_NAME;
    }

    protected CoreConfig config() {
        return config;
    }

    protected CoreLogback logback() {
        return logback;
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

    protected void start() throws TypeDBCheckedException {
        try {
            server.start();
            logger().info("{} is now running and will keep this process alive.", name());
            logger().info("You can press CTRL+C to shutdown this server.");
            logger().info("");
        } catch (IOException e) {
            if (e.getCause() != null && e.getCause() instanceof BindException) {
                throw TypeDBCheckedException.of(PORT_IN_USE, address());
            } else {
                throw TypeDBCheckedException.of(JAVA_ERROR, e);
            }
        }
    }

    protected void serve() {
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            // server is terminated
            try {
                close();
                System.exit(0);
            } catch (Throwable error) {
                logger().error("Unexpected error during shutdown, performing hard stop.", error);
                Runtime.getRuntime().halt(1);
            }
        }
    }

    @Override
    public synchronized void close() throws TypeDBCheckedException {
        if (isOpen.compareAndSet(true, false)) {
            try {
                logger().info("");
                logger().info("Closing {} server...", name());
                assert typeDBService != null;
                typeDBService.close();
                logger().info("Stopping network layer...");
                server.shutdown();
                if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
                logger().info("Stopping storage layer...");
                databaseMgr.close();
                System.runFinalization();
                logger().info("{} server has been closed.", name());
            } catch (Throwable e) {
                logger().error(FAILED_AT_STOPPING.message(), e);
                throw TypeDBCheckedException.of(UNKNOWN_ERROR, e);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        printASCIILogo();

        CoreConfigParser configParser = new CoreConfigParser();
        ArgsParser<CoreSubcommand> argsParser = new ArgsParser<CoreSubcommand>()
                .subcommand(new CoreSubcommandParser.Server(configParser))
                .subcommand(new CoreSubcommandParser.Import())
                .subcommand(new CoreSubcommandParser.Export());
        Optional<CoreSubcommand> subcmd = argsParser.parse(args);
        if (subcmd.isEmpty()) {
            LOG.error(UNRECOGNISED_CLI_COMMAND.message(String.join(" ", args)));
            LOG.error(argsParser.usage());
            System.exit(1);
        } else {
            if (subcmd.get().isServer()) {
                CoreSubcommand.Server subcmdServer = subcmd.get().asServer();
                if (subcmdServer.isHelp()) System.out.println(argsParser.help());
                else if (subcmdServer.isVersion()) {
                    System.out.println("Version: " + Version.VERSION);
                    System.exit(0);
                } else runServer(subcmdServer);
            } else if (subcmd.get().isImport()) {
                runImport(subcmd.get().asImport());
            } else if (subcmd.get().isExport()) {
                runExport(subcmd.get().asExport());
            } else throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    private static void runServer(CoreSubcommand.Server subcmdServer) {
        Instant start = Instant.now();
        TypeDBServer server = TypeDBServer.create(subcmdServer.config(), subcmdServer.isDebug());
        try {
            server.start();
        } catch (TypeDBCheckedException e) {
            server.logger().error(e.getMessage());
            System.exit(1);
        }
        Instant end = Instant.now();
        server.logger().info("version: {}", Version.VERSION);
        server.logger().info("listening to address: {}:{}", server.address().getHostString(), server.address().getPort());
        server.logger().info("data directory configured to: {}", server.dataDir());
        server.logger().info("bootup completed in: {} ms", Duration.between(start, end).toMillis());
        server.logger().info("");
        server.serve();
    }

    private static void runExport(CoreSubcommand.Export subcmdExport) {
        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(ROOT_LOGGER_NAME)
                .setLevel(ch.qos.logback.classic.Level.WARN);

        CoreMigratorClient migrator = CoreMigratorClient.create(subcmdExport.port());
        boolean success = migrator.exportDatabase(subcmdExport.database(), subcmdExport.schemaFile(), subcmdExport.dataFile());
        System.exit(success ? 0 : 1);
    }

    private static void runImport(CoreSubcommand.Import subcmdImport) {
        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(ROOT_LOGGER_NAME)
                .setLevel(ch.qos.logback.classic.Level.WARN);

        CoreMigratorClient migrator = CoreMigratorClient.create(subcmdImport.port());
        boolean success = migrator.importDatabase(subcmdImport.database(), subcmdImport.schemaFile(), subcmdImport.dataFile());
        System.exit(success ? 0 : 1);
    }
}
