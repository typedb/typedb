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

    private GraknConfig config;
    private Path dataDirTmp;
    private Server graknServer;

    private KeyspaceStore keyspaceStore;

    private int storagePort;
    private int rpcPort;
    private int nativeTransportPort;

    private EngineGraknTxFactory engineGraknTxFactory;

    public ConcurrentGraknServer() {
        System.setProperty("java.security.manager", "nottodaypotato");
    }


    @Override
    protected void before() {

        try {
            Path ciao = Paths.get("test-integration/resources/cassandra-embedded.yaml");
            byte[] bytes = Files.readAllBytes(ciao);
            String result = new String(bytes, StandardCharsets.UTF_8);
            storagePort = findUnusedLocalPort();
            nativeTransportPort = findUnusedLocalPort();
            rpcPort = findUnusedLocalPort();
            result = result + "\nstorage_port: " + storagePort;
            result = result + "\nnative_transport_port: " + nativeTransportPort;
            result = result + "\nrpc_port: " + rpcPort;
            InputStream pimped = new ByteArrayInputStream(result.getBytes(StandardCharsets.UTF_8));


            String directory = "target/embeddedCassandra";
            org.apache.cassandra.io.util.FileUtils.createDirectory(directory);
            Path copyName = Paths.get(directory, "cassandra-embedded.yaml");
            Files.copy(pimped, copyName);
            File file = copyName.toFile();

            System.setProperty("cassandra.config", "file:" + file.getAbsolutePath());
            System.setProperty("cassandra-foreground", "true");
            GraknCassandra.main(new String[]{});
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Cassandra started.");
        try {
            dataDirTmp = Files.createTempDirectory("db-for-test");
        } catch (IOException e) {
            throw new RuntimeException("Cannot create temp dir for Grakn data", e);
        }
        config = createTestConfig(dataDirTmp.toString());
        System.out.println("starting engine...");


        startGraknEngineServer();

    }

    private static String getConfigStringFromFile(Path configPath) {
        try {
            byte[] bytes = Files.readAllBytes(configPath);
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static int findUnusedLocalPort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }

    @Override
    protected void after() {
        engineGraknTxFactory.closeSessions();
        keyspaceStore.closeStore();
        try {
            graknServer.close();
            FileUtils.deleteDirectory(dataDirTmp.toFile());
        } catch (Exception e) {
            throw new RuntimeException("Could not shut down ", e);
        }
    }


    // GETTERS


    public SimpleURI grpcUri() {
        return new SimpleURI(config.getProperty(GraknConfigKey.SERVER_HOST_NAME), config.getProperty(GraknConfigKey.GRPC_PORT));
    }

    public EngineGraknTxFactory factory() {
        return engineGraknTxFactory;
    }


    private GraknConfig createTestConfig(String dataDir) {
        InputStream TEST_CONFIG_FILE = null;
        try {
            TEST_CONFIG_FILE = new FileInputStream("server/conf/grakn.properties");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        GraknConfig config = GraknConfig.read(TEST_CONFIG_FILE);
        config.setConfigProperty(GraknConfigKey.DATA_DIR, dataDir);
        //We override the default store.port with the RPC_PORT given that we still use Thrift protocol to talk to Cassandra
        config.setConfigProperty(GraknConfigKey.STORAGE_PORT, rpcPort);
        //Hadoop cluster uses the Astyanax driver for some operations, so need to override the RPC_PORT (Thrift)
        config.setConfigProperty(GraknConfigKey.HADOOP_STORAGE_PORT, rpcPort);
        //Hadoop cluster uses the CQL driver for some operations, so we need to instruct it to use the newly generate native transport port (CQL)
        config.setConfigProperty(GraknConfigKey.STORAGE_CQL_NATIVE_PORT, nativeTransportPort);

        return config;
    }

    public EmbeddedGraknSession sessionWithNewKeyspace() {
        Keyspace randomKeyspace = Keyspace.of("a"+ UUID.randomUUID().toString().replaceAll("-", ""));
        return EmbeddedGraknSession.createEngineSession(randomKeyspace, config);
    }

    private void startGraknEngineServer() {
        EngineID id = EngineID.me();
        ServerStatus status = new ServerStatus();

        // distributed locks
        LockProvider lockProvider = new ProcessWideLockProvider();

        keyspaceStore = new KeyspaceStoreImpl(config);

        // tx-factory
        engineGraknTxFactory = EngineGraknTxFactory.create(lockProvider, config, keyspaceStore);

        AttributeDeduplicatorDaemon attributeDeduplicatorDaemon = new AttributeDeduplicatorDaemon(config, engineGraknTxFactory);
        OpenRequest requestOpener = new ServerOpenRequest(engineGraknTxFactory);

        io.grpc.Server server = ServerBuilder.forPort(0)
                .addService(new SessionService(requestOpener, attributeDeduplicatorDaemon))
                .addService(new KeyspaceService(keyspaceStore))
                .build();
        ServerRPC rpcServerRPC = ServerRPC.create(server);

        Server graknEngineServer = ServerFactory.createServer(id, config, status, rpcServerRPC,
                lockProvider, attributeDeduplicatorDaemon, keyspaceStore);

        try {
            graknEngineServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Read the automatically allocated ports and write them back into the config
        config.setConfigProperty(GraknConfigKey.GRPC_PORT, server.getPort());

        graknServer = graknEngineServer;
    }

}
