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

package ai.grakn.test.rule;

import ai.grakn.GraknConfigKey;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.KeyspaceStore;
import ai.grakn.engine.Server;
import ai.grakn.engine.ServerFactory;
import ai.grakn.engine.ServerRPC;
import ai.grakn.engine.ServerStatus;
import ai.grakn.engine.attribute.deduplicator.AttributeDeduplicatorDaemon;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.lock.ProcessWideLockProvider;
import ai.grakn.engine.rpc.KeyspaceService;
import ai.grakn.engine.rpc.OpenRequest;
import ai.grakn.engine.rpc.ServerOpenRequest;
import ai.grakn.engine.rpc.SessionService;
import ai.grakn.engine.util.EngineID;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.keyspace.KeyspaceStoreImpl;
import ai.grakn.util.GraknTestUtil;
import ai.grakn.util.SimpleURI;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.jayway.restassured.RestAssured;
import io.grpc.ServerBuilder;
import org.apache.commons.io.FileUtils;
import org.junit.rules.TestRule;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.SampleKBLoader.randomKeyspace;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;


/**
 * <p>
 * Start the Grakn Engine server before each test class and stop after.
 *
 * NOTE: This Context only works with Janus profile and should only be used for Integration Tests.
 * </p>
 *
 * @author alexandraorth
 */
public class EngineContext extends CompositeTestRule {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EngineContext.class);

    /**
     * This is the equivalent of $GRAKN_HOME/db but for testing.
     *
     * Every instance of an EngineContext should have its own data dir and it is accomplished by using TemporaryFolder,
     * which is a JUnit class. Another bonus which it has is the guarantee that the folder will be deleted after the test
     * has finished.
     *
     * The dataDirTmp instance is listed as one of the rules under the testRules() method.
     *
     */
    private Path dataDirTmp;
    private Server server;
    private GraknConfig config;
    private spark.Service sparkHttp;

    public KeyspaceStore systemKeyspace() {
        return keyspaceStore;
    }

    private KeyspaceStore keyspaceStore;

    public EngineGraknTxFactory factory() {
        return engineGraknTxFactory;
    }

    private EngineGraknTxFactory engineGraknTxFactory;

    private EngineContext() {

    }

    /**
     * Creates a {@link EngineContext} for testing which uses an in-memory redis mock.
     *
     * @return a new {@link EngineContext} for testing
     */
    public static EngineContext create() {
        return new EngineContext();
    }

    public Server server() {
        return server;
    }

    public SimpleURI uri() {
        return config.uri();
    }

    public SimpleURI grpcUri() {
        return new SimpleURI(config.uri().getHost(), config.getProperty(GraknConfigKey.GRPC_PORT));
    }

    public EmbeddedGraknSession sessionWithNewKeyspace() {
        return EmbeddedGraknSession.createEngineSession(randomKeyspace(), config);
    }

    @Override
    protected final List<TestRule> testRules() {
        return ImmutableList.of(SessionContext.create());
    }

    @Override
    protected final void before() throws Throwable {
        dataDirTmp = Files.createTempDirectory("db-for-test");
        config = createTestConfig(dataDirTmp.toString());
        RestAssured.baseURI = uri().toURI().toString();
        if (!config.getProperty(GraknConfigKey.TEST_START_EMBEDDED_COMPONENTS)) {
            return;
        }

        // To ensure consistency b/w test profiles and configuration files, when not using Janus
        // for a unit tests in an IDE, add the following option:
        // -Dgrakn.conf=../conf/test/tinker/grakn.properties
        //
        // When using janus, add -Dgrakn.test-profile=janus
        //
        // The reason is that the default configuration of Grakn uses the Janus Factory while the default
        // test profile is tinker: so when running a unit test within an IDE without any extra parameters,
        // we end up wanting to use the JanusFactory but without starting Cassandra first.
        LOG.info("starting engine...");

        // start engine
        setRestAssuredUri(config);

        sparkHttp = spark.Service.ignite();

        server = startGraknEngineServer();

        LOG.info("engine started on " + uri());
    }

    @Override
    protected final void after() {
        if (!config.getProperty(GraknConfigKey.TEST_START_EMBEDDED_COMPONENTS)) {
            return;
        }

        try {
            noThrow(() -> {
                LOG.info("stopping engine...");

                // Clear graphs before closing the server because deleting keyspaces needs access to the rest endpoint
                clearGraphs();
                server.close();

                LOG.info("engine stopped.");

                // There is no way to stop the embedded Casssandra, no such API offered.
            }, "Error closing engine");
            sparkHttp.stop();
            FileUtils.deleteDirectory(dataDirTmp.toFile());
        } catch (Exception e) {
            throw new RuntimeException("Could not shut down ", e);
        }


    }

    private void clearGraphs() {
        // Drop all keyspaces
        final Set<String> keyspaceNames = new HashSet<String>();
        try (GraknTx systemGraph = engineGraknTxFactory.tx(KeyspaceStore.SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)) {
            systemGraph.graql().match(var("x").isa("keyspace-name"))
                    .forEach(x -> x.concepts().forEach(y -> {
                        keyspaceNames.add(y.asAttribute().value().toString());
                    }));
        }

        keyspaceNames.forEach(name -> keyspaceStore.deleteKeyspace(Keyspace.of(name)));
        engineGraknTxFactory.refreshConnections();
    }

    private static void noThrow(RunnableWithExceptions fn, String errorMessage) {
        try {
            fn.run();
        } catch (Throwable t) {
            LOG.error(errorMessage + "\nThe exception was: " + getFullStackTrace(t));
        }
    }

    /**
     * Function interface that throws exception for use in the noThrow function
     *
     * @param <E>
     */
    @FunctionalInterface
    private interface RunnableWithExceptions<E extends Exception> {
        void run() throws E;
    }

    /**
     * Create a configuration for use in tests, using random ports.
     */
    public static GraknConfig createTestConfig(String dataDir) {
        GraknConfig config = GraknConfig.create();
        config.setConfigProperty(GraknConfigKey.DATA_DIR, dataDir);
        config.setConfigProperty(GraknConfigKey.SERVER_PORT, 0);

        return config;
    }

    private static void setRestAssuredUri(GraknConfig config) {
        RestAssured.baseURI = "http://" + config.uri();
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
