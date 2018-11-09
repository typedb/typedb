package ai.grakn.test.rule;

import ai.grakn.GraknConfigKey;
import ai.grakn.Keyspace;
import ai.grakn.core.server.GraknConfig;
import ai.grakn.core.server.KeyspaceStore;
import ai.grakn.core.server.Server;
import ai.grakn.core.server.ServerFactory;
import ai.grakn.core.server.ServerRPC;
import ai.grakn.core.server.ServerStatus;
import ai.grakn.core.server.bootup.GraknCassandra;
import ai.grakn.core.server.deduplicator.AttributeDeduplicatorDaemon;
import ai.grakn.core.server.factory.EngineGraknTxFactory;
import ai.grakn.core.server.lock.LockProvider;
import ai.grakn.core.server.lock.ProcessWideLockProvider;
import ai.grakn.core.server.rpc.KeyspaceService;
import ai.grakn.core.server.rpc.OpenRequest;
import ai.grakn.core.server.rpc.ServerOpenRequest;
import ai.grakn.core.server.rpc.SessionService;
import ai.grakn.core.server.util.EngineID;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.keyspace.KeyspaceStoreImpl;
import ai.grakn.util.SimpleURI;
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
 * This rule starts Cassandra and Grakn Server and makes sure that both processes bind to random and unused ports.
 * This rule allows all the integration tests to run concurrently on the same machine.
 */
public class ConcurrentGraknServer extends ExternalResource {

    private final static String SERVER_CONFIG_PATH = "server/conf/grakn.properties";
    private final static Path CASSANDRA_CONFIG_PATH = Paths.get("test-integration/resources/cassandra-embedded.yaml");
    private GraknConfig serverConfig;
    private Path dataDirTmp;
    private Server graknServer;
    private KeyspaceStore keyspaceStore;

    private int storagePort;
    private int rpcPort;
    private int nativeTransportPort;
    private int grpcPort;

    private EngineGraknTxFactory engineGraknTxFactory;

    public ConcurrentGraknServer() {
        System.setProperty("java.security.manager", "nottodaypotato");
    }

    @Override
    protected void before() {
        try {
            //Start Cassandra with random ports
            generateCassandraRandomPorts();
            File cassandraConfig = buildCassandraConfigWithRandomPorts();
            System.setProperty("cassandra.config", "file:" + cassandraConfig.getAbsolutePath());
            System.setProperty("cassandra-foreground", "true");
            GraknCassandra.main(new String[]{});
            System.out.println("Cassandra started.");

            //Start Grakn server
            grpcPort = findUnusedLocalPort();
            dataDirTmp = Files.createTempDirectory("db-for-test");
            serverConfig = createTestConfig(dataDirTmp.toString());
            System.out.println("starting engine...");

            graknServer = createGraknServer();
            graknServer.start();
        } catch (IOException e) {
            throw new RuntimeException("Cannot start Server", e);
        }
    }

    @Override
    protected void after() {
        try {
            engineGraknTxFactory.closeSessions();
            keyspaceStore.closeStore();
            graknServer.close();
            FileUtils.deleteDirectory(dataDirTmp.toFile());
        } catch (Exception e) {
            throw new RuntimeException("Could not shut down ", e);
        }
    }


    // Getters
    public SimpleURI grpcUri() {
        return new SimpleURI(serverConfig.getProperty(GraknConfigKey.SERVER_HOST_NAME), serverConfig.getProperty(GraknConfigKey.GRPC_PORT));
    }

    public EmbeddedGraknSession sessionWithNewKeyspace() {
        Keyspace randomKeyspace = Keyspace.of("a" + UUID.randomUUID().toString().replaceAll("-", ""));
        return EmbeddedGraknSession.createEngineSession(randomKeyspace, serverConfig);
    }

    public EngineGraknTxFactory txFactory(){
        return engineGraknTxFactory;
    }


    //Cassandra Helpers
    private void generateCassandraRandomPorts() throws IOException {
        storagePort = findUnusedLocalPort();
        nativeTransportPort = findUnusedLocalPort();
        rpcPort = findUnusedLocalPort();
    }

    private File buildCassandraConfigWithRandomPorts() throws IOException {
        byte[] bytes = Files.readAllBytes(CASSANDRA_CONFIG_PATH);
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
    private GraknConfig createTestConfig(String dataDir) throws FileNotFoundException {
        InputStream testConfig = new FileInputStream(SERVER_CONFIG_PATH);

        GraknConfig config = GraknConfig.read(testConfig);
        config.setConfigProperty(GraknConfigKey.DATA_DIR, dataDir);
        //Override gRPC port with a random free port
        config.setConfigProperty(GraknConfigKey.GRPC_PORT, grpcPort);
        //Override the default store.port with the RPC_PORT given that we still use Thrift protocol to talk to Cassandra
        config.setConfigProperty(GraknConfigKey.STORAGE_PORT, rpcPort);
        //Hadoop cluster uses the Astyanax driver for some operations, so need to override the RPC_PORT (Thrift)
        config.setConfigProperty(GraknConfigKey.HADOOP_STORAGE_PORT, rpcPort);
        //Hadoop cluster uses the CQL driver for some operations, so we need to instruct it to use the newly generate native transport port (CQL)
        config.setConfigProperty(GraknConfigKey.STORAGE_CQL_NATIVE_PORT, nativeTransportPort);

        return config;
    }

    private Server createGraknServer() {
        EngineID id = EngineID.me();
        ServerStatus status = new ServerStatus();

        // distributed locks
        LockProvider lockProvider = new ProcessWideLockProvider();

        keyspaceStore = new KeyspaceStoreImpl(serverConfig);

        // tx-factory
        engineGraknTxFactory = EngineGraknTxFactory.create(lockProvider, serverConfig, keyspaceStore);

        AttributeDeduplicatorDaemon attributeDeduplicatorDaemon = new AttributeDeduplicatorDaemon(serverConfig, engineGraknTxFactory);
        OpenRequest requestOpener = new ServerOpenRequest(engineGraknTxFactory);

        io.grpc.Server server = ServerBuilder.forPort(grpcPort)
                .addService(new SessionService(requestOpener, attributeDeduplicatorDaemon))
                .addService(new KeyspaceService(keyspaceStore))
                .build();
        ServerRPC rpcServer = ServerRPC.create(server);

        return ServerFactory.createServer(id, serverConfig, status, rpcServer,
                lockProvider, attributeDeduplicatorDaemon, keyspaceStore);
    }

}
