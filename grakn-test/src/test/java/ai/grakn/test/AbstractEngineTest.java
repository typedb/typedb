/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknGraphFactory;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.backgroundtasks.distributed.ClusterManager;
import ai.grakn.engine.backgroundtasks.distributed.Scheduler;
import ai.grakn.engine.backgroundtasks.distributed.TaskRunner;
import ai.grakn.engine.backgroundtasks.taskstorage.GraknStateStorage;
import ai.grakn.engine.backgroundtasks.taskstorage.SynchronizedStateStorage;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.GraphFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.jayway.restassured.RestAssured;
import info.batey.kafka.unit.KafkaUnit;
import org.apache.commons.io.IOUtils;
import org.junit.BeforeClass;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static java.lang.Thread.sleep;

/**
 * Abstract test class that automatically starts the backend and engine and provides a method to get a graph factory
 */
public abstract class AbstractEngineTest {
    private static final String CONFIG = System.getProperty("grakn.test-profile");
    private static final Properties properties = ConfigProperties.getInstance().getProperties();
    private static AtomicBoolean ENGINE_ON = new AtomicBoolean(false);
    private static KafkaUnit kafkaUnit;

    private static void hideLogs() {
        Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.DEBUG);

        ((Logger) org.slf4j.LoggerFactory.getLogger(SynchronizedStateStorage.class)).setLevel(Level.DEBUG);
        ((Logger) org.slf4j.LoggerFactory.getLogger(TaskRunner.class)).setLevel(Level.DEBUG);
        ((Logger) org.slf4j.LoggerFactory.getLogger(Scheduler.class)).setLevel(Level.DEBUG);
        ((Logger) org.slf4j.LoggerFactory.getLogger(GraknStateStorage.class)).setLevel(Level.DEBUG);

        // Hide kafka logs
        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.ERROR);
    }

    @BeforeClass
    public static void startTestEngine() throws Exception {
        hideLogs();
        if (ENGINE_ON.compareAndSet(false, true)) {
            if (usingTitan()) {
                startEmbeddedCassandra();
            }

            kafkaUnit = new KafkaUnit(2181, 9092);
            kafkaUnit.startup();

            GraknEngineServer.start();
            sleep(5000);
        }

        RestAssured.baseURI = "http://" + properties.getProperty("server.host") + ":" + properties.getProperty("server.port");
    }

    protected static GraknGraphFactory factoryWithNewKeyspace() {
        String keyspace;
        if (usingOrientDB()) {
            keyspace = "memory";
        } else {
            keyspace = UUID.randomUUID().toString().replaceAll("-", "");
        }
        return Grakn.factory(Grakn.DEFAULT_URI, keyspace);
    }

    private static void startEmbeddedCassandra() {
        try {
            Class cl = Class.forName("org.cassandraunit.utils.EmbeddedCassandraServerHelper");

            hideLogs();

            //noinspection unchecked
            cl.getMethod("startEmbeddedCassandra", String.class).invoke(null, "cassandra-embedded.yaml");

            hideLogs();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static boolean usingTinker() {
        return "tinker".equals(CONFIG);
    }

    protected static boolean usingTitan() {
        return "titan".equals(CONFIG);
    }

    protected static boolean usingOrientDB() {
        return "orientdb".equals(CONFIG);
    }

    protected String getPath(String file) {
        return AbstractEngineTest.class.getResource("/"+file).getPath();
    }

    protected String readFileAsString(String file) {
        InputStream stream = AbstractEngineTest.class.getResourceAsStream("/"+file);

        try {
            return IOUtils.toString(stream);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void loadOntology(String file, String graphName) {
        try(GraknGraph graph = GraphFactory.getInstance().getGraph(graphName)) {

            String ontology = readFileAsString(file);

            graph.graql().parse(ontology).execute();
            graph.commit();

        } catch (GraknValidationException e){
            throw new RuntimeException(e);
        }
    }

    protected void waitForScheduler(ClusterManager clusterManager, Predicate<Scheduler> fn) throws Exception {
        int runs = 0;

        while (!fn.test(clusterManager.getScheduler()) && runs < 50 ) {
            Thread.sleep(100);
            runs++;
        }

        System.out.println("wait done, runs " + Integer.toString(runs) + " scheduler " + clusterManager.getScheduler());
    }
}
