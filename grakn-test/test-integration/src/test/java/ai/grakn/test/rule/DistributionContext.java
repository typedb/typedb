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

package ai.grakn.test.rule;

import ai.grakn.GraknConfigKey;
import ai.grakn.GraknSystemProperty;
import ai.grakn.client.Client;
import ai.grakn.engine.Grakn;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.util.GraknVersion;
import ai.grakn.util.SimpleURI;
import com.google.common.collect.ImmutableList;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.junit.Assert;
import org.junit.rules.TestRule;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.System.currentTimeMillis;
import static java.util.stream.Collectors.joining;

/**
 * Start a SingleQueueEngine from the packaged distribution.
 * This context can be used for integration tests.
 *
 * @author alexandraorth
 */
public class DistributionContext extends CompositeTestRule {

    private static final FilenameFilter jarFiles = (dir, name) -> name.toLowerCase().endsWith(".jar");
    private static final String ZIP = "grakn-dist-" + GraknVersion.VERSION + ".zip";
    private static final String CURRENT_DIRECTORY = GraknSystemProperty.PROJECT_RELATIVE_DIR.value();
    private static final String TARGET_DIRECTORY = CURRENT_DIRECTORY + "/grakn-dist/target/";
    private static final String DIST_DIRECTORY = TARGET_DIRECTORY + "grakn-dist-" + GraknVersion.VERSION;

    private Process engineProcess;
    private int port = 4567;
    private boolean inheritIO = true;
    private int redisPort = 6379;

    private DistributionContext(){
    }

    public static DistributionContext create(){
        return new DistributionContext();
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
        waitForEngine();
        return true;
    }

    public SimpleURI uri(){
        return new SimpleURI("localhost", port);
    }

    @Override
    protected List<TestRule> testRules() {
        return ImmutableList.of(
                SessionContext.create(),
                InMemoryRedisContext.create(redisPort)
        );
    }

    @Override
    public void before() throws Throwable {
        assertPackageBuilt();
        unzipDistribution();
        engineProcess = newEngineProcess(port, redisPort);
        waitForEngine();
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
        GraknEngineConfig config = GraknEngineConfig.create();
        config.setConfigProperty(GraknConfigKey.SERVER_PORT, port);
        config.setConfigProperty(GraknConfigKey.REDIS_HOST, ImmutableList.of(new SimpleURI("localhost", redisPort).toString()));
        // To speed up tests of failure cases
        config.setConfigProperty(GraknConfigKey.TASKS_RETRY_DELAY, 60);

        // Write new properties to disk
        File propertiesFile = new File("grakn-engine-" + port + ".properties");
        propertiesFile.deleteOnExit();
        config.write(propertiesFile);

        // Java commands to start Engine process
        String[] commands = {"java",
                "-cp", getClassPath(),
                "-Dgrakn.dir=" + DIST_DIRECTORY + "/services",
                "-Dgrakn.conf=" + propertiesFile.getAbsolutePath(),
                Grakn.class.getName(), "&"};

        // Start process
        ProcessBuilder processBuilder = new ProcessBuilder(commands);
        if (inheritIO) processBuilder.inheritIO();
        return processBuilder.start();
    }

    /**
     * Get the class path of all the jars in the /lib folder
     */
    private String getClassPath(){
        Stream<File> jars = Stream.of(new File(DIST_DIRECTORY + "/services/lib").listFiles(jarFiles));
        File conf = new File(DIST_DIRECTORY + "/conf/");
        File graknLogback = new File(DIST_DIRECTORY + "/services/grakn/");
        return Stream.concat(jars, Stream.of(conf, graknLogback))
                .filter(f -> !f.getName().contains("slf4j-log4j12"))
                .map(File::getAbsolutePath)
                .collect(joining(":"));
    }

    /**
     * Wait for the engine REST API to be available
     */
    private void waitForEngine() {
        long endTime = currentTimeMillis() + 120000;
        while (currentTimeMillis() < endTime) {
            if (Client.serverIsRunning(uri())) {
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