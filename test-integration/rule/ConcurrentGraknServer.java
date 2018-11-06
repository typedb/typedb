package ai.grakn.test.rule;

import ai.grakn.GraknConfigKey;
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
import ai.grakn.test.util.GraknTestUtil;
import ai.grakn.util.SimpleURI;
import com.codahale.metrics.MetricRegistry;
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
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This rule starts Cassandra and Grakn Server and makes sure that both processes bind to random and unused ports.
 * This rule allows all the integration tests to run concurrently on the same machine.
 */
public class ConcurrentGraknServer extends ExternalResource {

    private GraknConfig config;
    private Path dataDirTmp;
    private Server graknServer;
    private spark.Service sparkHttp;
    private ExecutorService executor;
    private ExecutorService serverExecutor;
    private RunnableServer serverRunnable;


    private KeyspaceStore keyspaceStore;

    private int storagePort;
    private int rpcPort;
    private int nativeTransportPort;

    private EngineGraknTxFactory engineGraknTxFactory;

    public ConcurrentGraknServer() {
        System.setProperty("java.security.manager", "nottodaypotato");
    }


    @Override
    protected void before() throws Throwable {
        executor = Executors.newSingleThreadExecutor();
        executor.execute(() -> {
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
                System.setProperty("cassandra.native.epoll.enabled", "false"); // JNA doesnt cope with relocated netty
                System.setProperty("cassandra.unsafesystem", "true"); // disable fsync for a massive speedup on old platters
                GraknCassandra.main(new String[]{});
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(5000);
        System.out.println("Cassandra started.");
        try {
            dataDirTmp = Files.createTempDirectory("db-for-test");
        } catch (IOException e) {
            throw new RuntimeException("Cannot create temp dir for Grakn data", e);
        }
        config = createTestConfig(dataDirTmp.toString());
        System.out.println("starting engine...");

        // start engine

        sparkHttp = spark.Service.ignite();

        startGraknEngineServer();

        System.out.println("engine started on " + uri());
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
        try {
            executor.shutdownNow();
            graknServer.close();
            sparkHttp.stop();
            serverRunnable.die();
            serverExecutor.shutdownNow();
            FileUtils.deleteDirectory(dataDirTmp.toFile());
        } catch (Exception e) {
            throw new RuntimeException("Could not shut down ", e);
        }
    }


    // GETTERS

    public SimpleURI uri() {
        return config.uri();
    }

    public SimpleURI grpcUri() {
        return new SimpleURI(config.uri().getHost(), config.getProperty(GraknConfigKey.GRPC_PORT));
    }

    public KeyspaceStore systemKeyspace() {
        return keyspaceStore;
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
        config.setConfigProperty(GraknConfigKey.SERVER_PORT, 0);
        //We override the default store.port with the RPC_PORT given that we still use Thrift protocol to talk to Cassandra
        config.setConfigProperty(GraknConfigKey.STORAGE_PORT, rpcPort);
        //Hadoop cluster uses the Astyanax driver for some operations, so need to override the RPC_PORT (Thrift)
        config.setConfigProperty(GraknConfigKey.HADOOP_STORAGE_PORT, rpcPort);
        //Hadoop cluster uses the CQL driver for some operations, so we need to instruct it to use the newly generate native transport port (CQL)
        config.setConfigProperty(GraknConfigKey.STORAGE_CQL_NATIVE_PORT, nativeTransportPort);

        return config;
    }

    public EmbeddedGraknSession sessionWithNewKeyspace() {
        return EmbeddedGraknSession.createEngineSession(GraknTestUtil.randomKeyspace(), config);
    }

    private void startGraknEngineServer() {
        serverRunnable = new RunnableServer();
        serverExecutor = Executors.newSingleThreadExecutor();
        serverExecutor.execute(serverRunnable);
    }

    class RunnableServer implements Runnable {

        public void die(){
            Thread.currentThread().interrupt();
        }

        @Override
        public void run() {
            EngineID id = EngineID.me();
            ServerStatus status = new ServerStatus();

            MetricRegistry metricRegistry = new MetricRegistry();

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
            GraknTestUtil.allocateSparkPort(config);

            Server graknEngineServer = ServerFactory.createServer(id, config, status,
                    sparkHttp, Collections.emptyList(), rpcServerRPC,
                    engineGraknTxFactory, metricRegistry,
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
}
