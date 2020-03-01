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

package grakn.core.rule;

import com.datastax.oss.driver.api.core.CqlSession;
import grakn.core.common.config.Config;
import grakn.core.common.config.ConfigKey;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.keyspace.Keyspace;
import grakn.core.server.Server;
import grakn.core.server.ServerFactory;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.keyspace.KeyspaceManager;
import grakn.core.server.rpc.KeyspaceRequestsHandler;
import grakn.core.server.rpc.KeyspaceService;
import grakn.core.server.rpc.OpenRequest;
import grakn.core.server.rpc.ServerKeyspaceRequestsHandler;
import grakn.core.server.rpc.ServerOpenRequest;
import grakn.core.server.rpc.SessionService;
import grakn.core.server.session.HadoopGraphFactory;
import grakn.core.server.session.JanusGraphFactory;
import grakn.core.server.session.SessionFactory;
import grakn.core.server.util.LockManager;
import io.grpc.ServerBuilder;
import org.junit.rules.ExternalResource;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * This rule is a test server rule which starts Cassandra and Grakn Core Server on random, unused ports.
 * It allows multiple test server instances to run concurrently.
 * It enables all of the integration tests to run concurrently on the same machine.
 */
public class GraknTestServer extends ExternalResource {
    private static final Path DEFAULT_SERVER_CONFIG_PATH = Paths.get("server/conf/grakn.properties");

    // Test Storage instance
    private final GraknTestStorage graknTestStorage;

    // Grakn Core Server
    private final Path serverConfigPath;
    protected Config serverConfig;
    protected Server graknServer;
    private int grpcPort;
    private SessionFactory sessionFactory;

    /**
     * Construct Grakn Server and Grakn Storage with default configurations
     */
    public GraknTestServer() {
        this.serverConfigPath = DEFAULT_SERVER_CONFIG_PATH;
        graknTestStorage = new GraknTestStorage();
    }

    /**
     * Construct Grakn Server and Grakn Storage with custom configurations
     */
    public GraknTestServer(Path serverConfigPath, Path cassandraConfigPath) {
        this.serverConfigPath = serverConfigPath;
        graknTestStorage = new GraknTestStorage(cassandraConfigPath);
    }

    @Override
    protected void before() {
        try {
            // Start Cassandra
            graknTestStorage.before();

            // half of this might be good to split into cassandra rule separately
            grpcPort = findUnusedLocalPort();
            serverConfig = createTestConfig();

            // Start Grakn Core Server
            System.out.println("Starting Grakn Core Server...");
            graknServer = createServer();
            graknServer.start();
            System.out.println("Grakn Core Server started");
        } catch (IOException e) {
            throw new RuntimeException("Cannot start components", e);
        }
    }

    @Override
    protected void after() {
        try {
            graknTestStorage.after();
            graknServer.close();
        } catch (Exception e) {
            throw new RuntimeException("Could not shut down ", e);
        }
    }

    // Getters

    public String grpcUri() {
        return serverConfig.getProperty(ConfigKey.SERVER_HOST_NAME) + ":" + serverConfig.getProperty(ConfigKey.GRPC_PORT);
    }

    public Session sessionWithNewKeyspace() {
        Keyspace randomKeyspace = randomKeyspaceName();
        return session(randomKeyspace);
    }

    public Keyspace randomKeyspaceName() {
        return new KeyspaceImpl("a" + UUID.randomUUID().toString().replaceAll("-", ""));
    }

    public Session session(Keyspace keyspace) {
        return sessionFactory.session(keyspace);
    }

    public SessionFactory sessionFactory() {
        return sessionFactory;
    }

    public Config serverConfig() {
        return serverConfig;
    }


    private synchronized static int findUnusedLocalPort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }

    // Grakn Core Server helpers

    private Config createTestConfig() throws FileNotFoundException {
        InputStream testConfig = new FileInputStream(serverConfigPath.toFile());

        Config config = Config.read(testConfig);
        //Override gRPC port with a random free port
        config.setConfigProperty(ConfigKey.STORAGE_HOSTNAME, "127.0.0.1");
        config.setConfigProperty(ConfigKey.GRPC_PORT, grpcPort);
        //Override Storage Port used by Janus to communicate with Cassandra Backend
        config.setConfigProperty(ConfigKey.STORAGE_PORT, graknTestStorage.nativeTransportPort());
        //Override ports used by HadoopGraph
        config.setConfigProperty(ConfigKey.HADOOP_STORAGE_PORT, graknTestStorage.nativeTransportPort());

        return config;
    }

    private Server createServer() {
        // distributed locks
        LockManager lockManager = new LockManager();
        JanusGraphFactory janusGraphFactory = new JanusGraphFactory(serverConfig);
        HadoopGraphFactory hadoopGraphFactory = new HadoopGraphFactory(serverConfig);

        Integer storagePort = serverConfig.getProperty(ConfigKey.STORAGE_PORT);
        String storageHostname = serverConfig.getProperty(ConfigKey.STORAGE_HOSTNAME);
        // CQL cluster used by KeyspaceManager to fetch all existing keyspaces
        CqlSession cqlSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(storageHostname, storagePort))
                .withLocalDatacenter("datacenter1")
                .build();

        KeyspaceManager keyspaceManager = new KeyspaceManager(cqlSession);
        sessionFactory = new SessionFactory(lockManager, janusGraphFactory, hadoopGraphFactory, serverConfig);

        OpenRequest requestOpener = new ServerOpenRequest(sessionFactory);

        KeyspaceRequestsHandler requestsHandler = new ServerKeyspaceRequestsHandler(
                keyspaceManager, sessionFactory, janusGraphFactory);

        io.grpc.Server serverRPC = ServerBuilder.forPort(grpcPort)
                .addService(new SessionService(requestOpener))
                .addService(new KeyspaceService(requestsHandler))
                .build();

        return ServerFactory.createServer(serverRPC);
    }

}
