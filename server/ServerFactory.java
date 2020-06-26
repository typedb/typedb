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
 *
 */

package grakn.core.server;

import com.datastax.oss.driver.api.core.CqlSession;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Parser;
import grabl.tracing.client.GrablTracing;
import grabl.tracing.client.GrablTracingThreadStatic;
import grakn.core.common.config.Config;
import grakn.core.common.config.ConfigKey;
import grakn.core.common.config.SystemProperty;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.keyspace.Keyspace;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.keyspace.KeyspaceManager;
import grakn.core.server.migrate.Export;
import grakn.core.server.migrate.Import;
import grakn.core.server.migrate.proto.MigrateProto;
import grakn.core.server.rpc.KeyspaceService;
import grakn.core.server.rpc.OpenRequest;
import grakn.core.server.rpc.ServerOpenRequest;
import grakn.core.server.rpc.SessionService;
import grakn.core.server.session.HadoopGraphFactory;
import grakn.core.server.session.JanusGraphFactory;
import grakn.core.server.session.SessionFactory;
import grakn.core.server.util.LockManager;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This is a factory class which contains methods for instantiating a Server in different ways.
 */
public class ServerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ServerFactory.class);

    private static final int GRPC_EXECUTOR_THREADS = Runtime.getRuntime().availableProcessors() * 2; // default Netty way of assigning threads, probably expose in config in future
    private static final int ELG_THREADS = 4; // this could also be 1, but to avoid risks set it to 4, probably expose in config in future

    /**
     * Create a Server configured for Grakn Core.
     *
     * @return a Server instance configured for Grakn Core
     */
    public static Server createServer(Grakn.Arguments arguments) {
        // Grakn Server configuration
        Config config = Config.create();

        JanusGraphFactory janusGraphFactory = new JanusGraphFactory(config);

        // locks
        LockManager lockManager = new LockManager();

        Integer storagePort = config.getProperty(ConfigKey.STORAGE_PORT);
        String storageHostname = config.getProperty(ConfigKey.STORAGE_HOSTNAME);
        // CQL cluster used by KeyspaceManager to fetch all existing keyspaces
        CqlSession cqlSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(storageHostname, storagePort))
                .withLocalDatacenter("datacenter1")
                .build();

        HadoopGraphFactory hadoopGraphFactory = new HadoopGraphFactory(config);
        SessionFactory sessionFactory = new SessionFactory(lockManager, janusGraphFactory, hadoopGraphFactory, config);
        KeyspaceManager keyspaceManager = new KeyspaceManager(cqlSession, janusGraphFactory, sessionFactory);

        if (arguments.isGrablTracing()) {
            LOG.info("Grabl tracing is enabled!");

            GrablTracing grablTracingClient;
            if (arguments.getGrablTracing().isSecureMode()) {
                LOG.info("Using Grabl tracing secure mode");
                grablTracingClient = GrablTracing.withLogging(GrablTracing.tracing(
                        arguments.getGrablTracing().getUri(),
                        arguments.getGrablTracing().getUsername(),
                        arguments.getGrablTracing().getAccessToken()
                ));
            } else {
                LOG.warn("Using Grabl tracing UNSECURED mode!!!");
                grablTracingClient = GrablTracing.withLogging(GrablTracing.tracing(arguments.getGrablTracing().getUri()));
            }
            GrablTracingThreadStatic.setGlobalTracingClient(grablTracingClient);
            LOG.info("Completed tracing setup");
        }

        keyspaceManager.keyspaces().forEach(keyspace -> {

            Path backupPath = Paths.get(SystemProperty.CURRENT_DIRECTORY.value(), "backup", keyspace.name() + ".grakn");

            try (Export export = new Export(sessionFactory.session(keyspace), Paths.get(SystemProperty.CURRENT_DIRECTORY.value(), "backup"))) {
                export.export();

                InputStream input = new BufferedInputStream(Files.newInputStream(backupPath));

                Parser<MigrateProto.Item> parser = MigrateProto.Item.parser();

                MigrateProto.Item item;
                while ((item =parser.parseDelimitedFrom(input)) != null) {
                    LOG.info(item.toString());
                }
            } catch (IOException e) {
                LOG.error("An error occurred during export testing.", e);
            }

            try (Import anImport = new Import(sessionFactory.session(keyspace), backupPath)) {
                anImport.execute();
            } catch (IOException e) {
                LOG.error("An error occurred during import testing.", e);
            }
        });

        // create gRPC server
        io.grpc.Server serverRPC = createServerRPC(config, sessionFactory, keyspaceManager);

        return createServer(serverRPC);
    }

    /**
     * Allows the creation of a Server instance with various configurations
     *
     * @return a Server instance
     */

    public static Server createServer(io.grpc.Server rpcServer) {
        Server server = new Server(rpcServer);

        Runtime.getRuntime().addShutdownHook(new Thread(server::close, "grakn-server-shutdown"));

        return server;
    }

    /**
     * Build a GrpcServer using the Netty default builder.
     * The Netty builder accepts 3 thread executors (threadpools):
     * - Boss Event Loop Group  (a.k.a. bossEventLoopGroup() )
     * - Worker Event Loop Group ( a.k.a. workerEventLoopGroup() )
     * - Application Executor (a.k.a. executor() )
     * <p>
     * The Boss group can be the same as the worker group.
     * It's purpose is to accept calls from the network, and create Netty channels (not gRPC Channels) to handle the socket.
     * <p>
     * Once the Netty channel has been created it gets passes to the Worker Event Loop Group.
     * This is the threadpool dedicating to doing socket read() and write() calls.
     * <p>
     * The last thread group is the application executor, also called the "app thread".
     * This is where the gRPC stubs do their main work.
     * It is for handling the callbacks that bubble up from the network thread.
     * <p>
     * Note from grpc-java developers:
     * Most people should use either reuse the same boss event loop group as the worker group.
     * Barring this, the boss eventloop group should be a single thread, since it does very little work.
     * For the app thread, users should provide a fixed size thread pool, as the default unbounded cached threadpool
     * is not the most efficient, and can be dangerous in some circumstances.
     * <p>
     * More info here: https://groups.google.com/d/msg/grpc-io/LrnAbWFozb0/VYCVarkWBQAJ
     */
    private static io.grpc.Server createServerRPC(Config config, SessionFactory sessionFactory, KeyspaceManager keyspaceManager) {
        int grpcPort = config.getProperty(ConfigKey.GRPC_PORT);
        OpenRequest requestOpener = new ServerOpenRequest(sessionFactory);

        SessionService sessionService = new SessionService(requestOpener);

        Runtime.getRuntime().addShutdownHook(new Thread(sessionService::shutdown, "session-service-shutdown"));

        NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(ELG_THREADS, new ThreadFactoryBuilder().setNameFormat("grpc-ELG-handler-%d").build());

        ExecutorService grpcExecutorService = Executors.newFixedThreadPool(GRPC_EXECUTOR_THREADS, new ThreadFactoryBuilder().setNameFormat("grpc-request-handler-%d").build());

        return NettyServerBuilder.forPort(grpcPort)
                .executor(grpcExecutorService)
                .workerEventLoopGroup(eventLoopGroup)
                .bossEventLoopGroup(eventLoopGroup)
                .maxConnectionIdle(1, TimeUnit.HOURS)
                .channelType(NioServerSocketChannel.class)
                .addService(sessionService)
                .addService(new KeyspaceService(keyspaceManager))
                .build();
    }

}