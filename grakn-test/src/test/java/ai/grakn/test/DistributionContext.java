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

import ai.grakn.client.Client;
import ai.grakn.engine.GraknEngineConfig;
import static ai.grakn.engine.GraknEngineConfig.REDIS_HOST;
import static ai.grakn.engine.GraknEngineConfig.SERVER_PORT_NUMBER;
import static ai.grakn.engine.GraknEngineConfig.TASKS_RETRY_DELAY;
import static ai.grakn.engine.GraknEngineConfig.TASK_MANAGER_IMPLEMENTATION;
import ai.grakn.engine.tasks.manager.StandaloneTaskManager;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.tasks.manager.redisqueue.RedisTaskManager;
import ai.grakn.engine.util.SimpleURI;
import ai.grakn.util.GraknVersion;
import com.google.common.base.StandardSystemProperty;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import static java.lang.System.currentTimeMillis;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import static java.util.stream.Collectors.joining;
import java.util.stream.Stream;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.junit.Assert;
import org.junit.rules.ExternalResource;

/**
 * Start a SingleQueueEngine from the packaged distribution.
 * This context can be used for integration tests.
 *
 * @author alexandraorth
 */
public class DistributionContext extends ExternalResource {

    private static final FilenameFilter jarFiles = (dir, name) -> name.toLowerCase().endsWith(".jar");
    private static final String ZIP = "grakn-dist-" + GraknVersion.VERSION + ".zip";
    private static final String CURRENT_DIRECTORY = StandardSystemProperty.USER_DIR.value();
    private static final String TARGET_DIRECTORY = CURRENT_DIRECTORY + "/../grakn-dist/target/";
    private static final String DIST_DIRECTORY = TARGET_DIRECTORY + "grakn-dist-" + GraknVersion.VERSION;
    private final Class<? extends TaskManager> taskManagerClass;

    private Process engineProcess;
    private int port = 4567;
    private boolean inheritIO = true;
    private int redisPort = 6379;

    private DistributionContext(Class<? extends TaskManager> taskManagerClass){
        this.taskManagerClass = taskManagerClass;
    }

    public static DistributionContext startSingleQueueEngineProcess(){
        return new DistributionContext(RedisTaskManager.class);
    }

    public static DistributionContext startInMemoryEngineProcess(){
        return new DistributionContext(StandaloneTaskManager.class);
    }

    public DistributionContext port(int port) {
        this.port = port;
        return this;
    }

    public DistributionContext inheritIO(boolean inheritIO) {
        this.inheritIO = inheritIO;
        return this;
    }

    public boolean restart() throws IOException {
        boolean isStarted = engineProcess != null && engineProcess.isAlive();
        if(!isStarted){
            return false;
        }

        engineProcess.destroyForcibly();
        engineProcess = newEngineProcess(port, redisPort);
        waitForEngine(port);
        return true;
    }

    public int port(){
        return port;
    }

    @Override
    public void before() throws Throwable {
        assertPackageBuilt();
        unzipDistribution();
        GraknTestSetup.startCassandraIfNeeded();
        GraknTestSetup.startRedisIfNeeded(redisPort);
        engineProcess = newEngineProcess(port, redisPort);
        waitForEngine(port);
    }

    @Override
    public void after() {
        engineProcess.destroyForcibly();
    }

    private void assertPackageBuilt() throws IOException {
        boolean packaged = Files.exists(Paths.get(TARGET_DIRECTORY, ZIP));

        if(!packaged) {
            Assert.fail("Grakn has not been packaged. Please package before running tests with the distribution context.");
        }
    }

    private void unzipDistribution() throws ZipException, IOException {
        // Unzip the distribution
        ZipFile zipped = new ZipFile( TARGET_DIRECTORY + ZIP);
        zipped.extractAll(TARGET_DIRECTORY);
    }

    private Process newEngineProcess(Integer port, Integer redisPort) throws IOException {
        // Set correct port & task manager
        Properties properties = GraknEngineConfig.create().getProperties();
        properties.setProperty(SERVER_PORT_NUMBER, port.toString());
        properties.setProperty(REDIS_HOST, new SimpleURI("localhost", redisPort).toString());
        properties.setProperty(TASK_MANAGER_IMPLEMENTATION, taskManagerClass.getName());
        // To speed up tests of failure cases
        properties.setProperty(TASKS_RETRY_DELAY, "60");

        // Write new properties to disk
        File propertiesFile = new File("grakn-engine-" + port + ".properties");
        propertiesFile.deleteOnExit();
        try(FileOutputStream os = new FileOutputStream(propertiesFile)) {
            properties.store(os, null);
        }

        // Java commands to start Engine process
        String[] commands = {"java",
                "-cp", getClassPath(),
                "-Dgrakn.dir=" + DIST_DIRECTORY + "/bin",
                "-Dgrakn.conf=" + propertiesFile.getAbsolutePath(),
                "ai.grakn.engine.GraknEngineServer", "&"};

        // Start process
        ProcessBuilder processBuilder = new ProcessBuilder(commands);
        if (inheritIO) processBuilder.inheritIO();
        return processBuilder.start();
    }

    /**
     * Get the class path of all the jars in the /lib folder
     */
    private String getClassPath(){
        Stream<File> jars = Stream.of(new File(DIST_DIRECTORY + "/lib").listFiles(jarFiles));
        File conf = new File(DIST_DIRECTORY + "/conf/main/");

        return Stream.concat(jars, Stream.of(conf))
                .filter(f -> !f.getName().contains("slf4j-log4j12"))
                .map(File::getAbsolutePath)
                .collect(joining(":"));
    }

    /**
     * Wait for the engine REST API to be available
     */
    private static void waitForEngine(int port) {
        long endTime = currentTimeMillis() + 60000;
        while (currentTimeMillis() < endTime) {
            if (Client.serverIsRunning("localhost:" + port)) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        throw new RuntimeException("Could not start engine within expected time");
    }

    public DistributionContext redisPort(int port) {
        this.redisPort = port;
        return this;
    }
}