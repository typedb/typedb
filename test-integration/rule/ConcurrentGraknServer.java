package ai.grakn.test.rule;

import ai.grakn.GraknConfigKey;
import ai.grakn.core.server.GraknConfig;
import ai.grakn.core.server.KeyspaceStore;
import ai.grakn.core.server.Server;
import ai.grakn.core.server.ServerFactory;
import ai.grakn.core.server.ServerRPC;
import ai.grakn.core.server.ServerStatus;
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
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

/**
 * This rule starts Cassandra and Grakn Server and makes sure that both processes bind to random and unused ports.
 * This rule allows all the integration tests to run concurrently on the same machine.
 */
public class ConcurrentGraknServer extends ExternalResource {

    private final InputStream TEST_CONFIG_FILE = ConcurrentGraknServer.class.getClassLoader().getResourceAsStream("server/conf/grakn.properties");
    private GraknConfig config;
    private Path dataDirTmp;
    private Server server;
    private spark.Service sparkHttp;

    private KeyspaceStore keyspaceStore;


    private EngineGraknTxFactory engineGraknTxFactory;

    public ConcurrentGraknServer() {
        System.setProperty("java.security.manager", "nottodaypotato");
    }

    @Override
    protected void before() throws Throwable{
        try {
            // Start Cassandra with config file for random ports.
            EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE, 30_000L);
        } catch (TTransportException | IOException | ConfigurationException e) {
            throw new RuntimeException("Cannot start Embedded Cassandra", e);
        }
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

        server = startGraknEngineServer();

        System.out.println("engine started on " + uri());
    }

    @Override
    protected void after() {
        try {
            server.close();
            sparkHttp.stop();
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
        GraknConfig config = GraknConfig.read(TEST_CONFIG_FILE);
        config.setConfigProperty(GraknConfigKey.DATA_DIR, dataDir);
        config.setConfigProperty(GraknConfigKey.SERVER_PORT, 0);
        //We override the default store.port with the RPC_PORT given that we still use Thrift protocol to talk to Cassandra
        config.setConfigProperty(GraknConfigKey.STORAGE_PORT, EmbeddedCassandraServerHelper.getRpcPort());
        //Hadoop cluster uses the Astyanax driver for some operations, so need to override the RPC_PORT (Thrift)
        config.setConfigProperty(GraknConfigKey.HADOOP_STORAGE_PORT, EmbeddedCassandraServerHelper.getRpcPort());
        //Hadoop cluster uses the CQL driver for some operations, so we need to instruct it to use the newly generate native transport port (CQL)
        config.setConfigProperty(GraknConfigKey.STORAGE_CQL_NATIVE_PORT, EmbeddedCassandraServerHelper.getNativeTransportPort());

        return config;
    }

    public EmbeddedGraknSession sessionWithNewKeyspace(){
        return EmbeddedGraknSession.createEngineSession(GraknTestUtil.randomKeyspace(), config);
    }

    private Server startGraknEngineServer() throws IOException {
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

        graknEngineServer.start();

        // Read the automatically allocated ports and write them back into the config
        config.setConfigProperty(GraknConfigKey.GRPC_PORT, server.getPort());

        return graknEngineServer;
    }

}
