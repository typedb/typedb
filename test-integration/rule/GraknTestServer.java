/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

import grakn.core.common.config.Config;
import grakn.core.common.config.ConfigKey;
import grakn.core.common.http.SimpleURI;
import grakn.core.server.GraknStorage;
import grakn.core.server.Server;
import grakn.core.server.ServerFactory;
import grakn.core.server.deduplicator.AttributeDeduplicatorDaemon;
import grakn.core.server.keyspace.Keyspace;
import grakn.core.server.keyspace.KeyspaceManager;
import grakn.core.server.rpc.KeyspaceService;
import grakn.core.server.rpc.OpenRequest;
import grakn.core.server.rpc.ServerOpenRequest;
import grakn.core.server.rpc.SessionService;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.SessionStore;
import grakn.core.server.util.LockManager;
import grakn.core.server.util.ServerID;
import grakn.core.server.util.ServerLockManager;
import io.grpc.ServerBuilder;
import org.apache.commons.io.FileUtils;
import org.junit.rules.ExternalResource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * This rule starts Cassandra and Grakn Server concurrently.
 * It makes sure that both processes bind to random and unused ports.
 * It allows all the integration tests to run concurrently on the same machine.
 */
public class GraknTestServer extends ExternalResource {
    private static final Path SERVER_CONFIG =  Paths.get("server/conf/grakn.properties");
    private static final Path CASSANDRA_CONFIG = Paths.get("test-integration/resources/cassandra-embedded.yaml");

    private final Path serverConfigPath;
    private final Path cassandraConfigPath;
    private Config serverConfig;
    private Path dataDirTmp;
    private Server graknServer;
    private KeyspaceManager keyspaceStore;

    private int storagePort;
    private int rpcPort;
    private int nativeTransportPort;
    private int grpcPort;

    private SessionStore sessionStore;

    public GraknTestServer() {
        this(SERVER_CONFIG, CASSANDRA_CONFIG);
    }

    public GraknTestServer(Path serverConfigPath, Path cassandraConfigPath) {
        System.setProperty("java.security.manager", "nottodaypotato");
        this.serverConfigPath = serverConfigPath;
        this.cassandraConfigPath = cassandraConfigPath;
    }

    @Override
    protected void before() {
        try {
            //Start Cassandra with random ports
            System.out.println("Starting Grakn Storage...");
            generateCassandraRandomPorts();
            File cassandraConfig = buildCassandraConfigWithRandomPorts();
            System.setProperty("cassandra.config", "file:" + cassandraConfig.getAbsolutePath());
            System.setProperty("cassandra-foreground", "true");
            GraknStorage.main(new String[]{});
            System.out.println("Grakn Storage started");

            //Start Grakn server
            grpcPort = findUnusedLocalPort();
            dataDirTmp = Files.createTempDirectory("db-for-test");
            serverConfig = createTestConfig(dataDirTmp.toString());
            System.out.println("Starting Grakn Server...");

            graknServer = createServer();
            graknServer.start();
            System.out.println("Grakn Server started");
        } catch (IOException e) {
            throw new RuntimeException("Cannot start Grakn Server", e);
        }
    }

    @Override
    protected void after() {
        try {
            sessionStore.closeSessions();
            keyspaceStore.closeStore();
            graknServer.close();
            FileUtils.deleteDirectory(dataDirTmp.toFile());
        } catch (Exception e) {
            throw new RuntimeException("Could not shut down ", e);
        }
    }


    // Getters
    public SimpleURI grpcUri() {
        return new SimpleURI(serverConfig.getProperty(ConfigKey.SERVER_HOST_NAME), serverConfig.getProperty(ConfigKey.GRPC_PORT));
    }

    public SessionImpl sessionWithNewKeyspace() {
        Keyspace randomKeyspace = Keyspace.of("a" + UUID.randomUUID().toString().replaceAll("-", ""));
        return SessionImpl.create(randomKeyspace, serverConfig);
    }

    public SessionStore txFactory(){
        return sessionStore;
    }


    //Cassandra Helpers
    private void generateCassandraRandomPorts() throws IOException {
        storagePort = findUnusedLocalPort();
        nativeTransportPort = findUnusedLocalPort();
        rpcPort = findUnusedLocalPort();
    }

    private File buildCassandraConfigWithRandomPorts() throws IOException {
        byte[] bytes = Files.readAllBytes(cassandraConfigPath);
        String configString = new String(bytes, StandardCharsets.UTF_8);

        configString = configString + "\nstorage_port: " + storagePort;
        configString = configString + "\nnative_transport_port: " + nativeTransportPort;
        configString = configString + "\nrpc_port: " + rpcPort;
        InputStream configStream = new ByteArrayInputStream(configString.getBytes(StandardCharsets.UTF_8));

        String directory = "target/embeddedCassandra";
        org.apache.cassandra.io.util.FileUtils.createDirectory(directory);
        Path copyName = Paths.get(directory, "cassandra-embedded.yaml");
        // Create file in directory we just created and copy the stream content into it.
        Files.copy(configStream, copyName);
        return copyName.toFile();
    }

    private static int findUnusedLocalPort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }

    //Server helpers
    private Config createTestConfig(String dataDir) throws FileNotFoundException {
        InputStream testConfig = new FileInputStream(serverConfigPath.toFile());

        Config config = Config.read(testConfig);
        config.setConfigProperty(ConfigKey.DATA_DIR, dataDir);
        //Override gRPC port with a random free port
        config.setConfigProperty(ConfigKey.GRPC_PORT, grpcPort);
        //Override the default store.port with the RPC_PORT given that we still use Thrift protocol to talk to Cassandra
        config.setConfigProperty(ConfigKey.STORAGE_PORT, rpcPort);
        //Hadoop cluster uses the Astyanax driver for some operations, so need to override the RPC_PORT (Thrift)
        config.setConfigProperty(ConfigKey.HADOOP_STORAGE_PORT, rpcPort);
        //Hadoop cluster uses the CQL driver for some operations, so we need to instruct it to use the newly generate native transport port (CQL)
        config.setConfigProperty(ConfigKey.STORAGE_CQL_NATIVE_PORT, nativeTransportPort);

        return config;
    }

    private Server createServer() {
        ServerID id = ServerID.me();

        // distributed locks
        LockManager lockManager = new ServerLockManager();

        keyspaceStore = new KeyspaceManager(serverConfig);

        // tx-factory
        sessionStore = SessionStore.create(lockManager, serverConfig, keyspaceStore);

        AttributeDeduplicatorDaemon attributeDeduplicatorDaemon = new AttributeDeduplicatorDaemon(serverConfig, sessionStore);
        OpenRequest requestOpener = new ServerOpenRequest(sessionStore);

        io.grpc.Server serverRPC = ServerBuilder.forPort(grpcPort)
                .addService(new SessionService(requestOpener, attributeDeduplicatorDaemon))
                .addService(new KeyspaceService(keyspaceStore))
                .build();

        return ServerFactory.createServer(id, serverConfig, serverRPC,
                                          lockManager, attributeDeduplicatorDaemon, keyspaceStore);
    }

}
