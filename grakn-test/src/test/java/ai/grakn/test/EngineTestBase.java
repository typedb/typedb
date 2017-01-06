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

import ai.grakn.GraknGraph;

import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.backgroundtasks.distributed.ClusterManager;
import ai.grakn.engine.backgroundtasks.distributed.DistributedTaskManager;
import ai.grakn.engine.backgroundtasks.distributed.Scheduler;
import ai.grakn.engine.backgroundtasks.distributed.TaskRunner;
import ai.grakn.engine.backgroundtasks.taskstorage.GraknStateStorage;
import ai.grakn.engine.backgroundtasks.taskstorage.SynchronizedStateStorage;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.GraphFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.auth0.jwt.internal.org.apache.commons.io.FileUtils;
import com.esotericsoftware.minlog.Log;
import com.jayway.restassured.RestAssured;
import info.batey.kafka.unit.KafkaUnit;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static ai.grakn.engine.util.ConfigProperties.TASK_MANAGER_INSTANCE;
import static ai.grakn.engine.util.ExceptionWrapper.noThrow;
import static java.lang.Thread.sleep;
import static ai.grakn.test.GraknTestEnv.*;

public class EngineTestBase {
    private static final Properties properties = ConfigProperties.getInstance().getProperties();
    private static AtomicBoolean ENGINE_ON = new AtomicBoolean(false);
    private static KafkaUnit kafkaUnit;
    private static Path tempDirectory;

    @BeforeClass
    public static void startTestEngine() {
    	try {
	        if(ENGINE_ON.compareAndSet(false, true)) {
	            System.out.println("STARTING ENGINE...");
	
	            hideLogs();
	            startEngineImpl();
	            RestAssured.baseURI = "http://" + properties.getProperty("server.host") + ":" + properties.getProperty("server.port");
	
	            System.out.println("STARTED ENGINE.");
	        }
    	}
    	catch (Exception ex) {
    		ex.printStackTrace(System.err);
    		Log.error("While starting Engine for test", ex);
    	}
    }

    private static void startEngineImpl() throws Exception {
        kafkaUnit = new KafkaUnit(2181, 9092);
        
        tempDirectory = Files.createTempDirectory("graknKafkaUnit " + UUID.randomUUID());
        kafkaUnit.setKafkaBrokerConfig("log.dirs", tempDirectory.toString());
        kafkaUnit.startup();

        ConfigProperties.getInstance().setConfigProperty(TASK_MANAGER_INSTANCE, DistributedTaskManager.class.getName());
        ensureEngineRunning();

        GraknEngineServer.startCluster();
        sleep(5000);
    }

    @AfterClass
    public static void stopTestEngine() throws Exception {
        if(ENGINE_ON.compareAndSet(true, false)) {
            System.out.println("STOPPING ENGINE...");

            noThrow(GraknEngineServer::stopCluster, "Problem while shutting down Zookeeper cluster.");            
            noThrow(kafkaUnit::shutdown, "Problem while shutting down Kafka Unit.");
            noThrow(GraknTestEnv::clearGraphs, "Problem while clearing graphs.");
            noThrow(GraknTestEnv::shutdownEngine, "Problem while shutting down Engine");

            sleep(5000);
            FileUtils.deleteDirectory(tempDirectory.toFile());

            System.out.println("ENGINE STOPPED.");
        }
    }

    private static void hideLogs() {
        Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);
        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.ERROR);

//        ((Logger) org.slf4j.LoggerFactory.getLogger(SynchronizedStateStorage.class)).setLevel(Level.DEBUG);
//        ((Logger) org.slf4j.LoggerFactory.getLogger(TaskRunner.class)).setLevel(Level.DEBUG);
//        ((Logger) org.slf4j.LoggerFactory.getLogger(Scheduler.class)).setLevel(Level.DEBUG);
//        ((Logger) org.slf4j.LoggerFactory.getLogger(GraknStateStorage.class)).setLevel(Level.DEBUG);
    }

    protected String getPath(String file) {
        return AbstractGraknTest.class.getResource("/"+file).getPath();
    }

    public static String readFileAsString(String file) {
        InputStream stream = AbstractGraknTest.class.getResourceAsStream("/"+file);

        try {
            return IOUtils.toString(stream);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void loadOntology(String file, String graphName) {
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
