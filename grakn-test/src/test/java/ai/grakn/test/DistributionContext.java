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
import ai.grakn.engine.tasks.manager.singlequeue.SingleQueueTaskManager;
import ai.grakn.util.GraknVersion;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.junit.rules.ExternalResource;

import static ai.grakn.engine.GraknEngineConfig.LOGGING_LEVEL;
import static ai.grakn.engine.GraknEngineConfig.SERVER_PORT_NUMBER;
import static ai.grakn.engine.GraknEngineConfig.TASK_MANAGER_IMPLEMENTATION;
import static ai.grakn.test.GraknTestEnv.startKafka;
import static ai.grakn.test.GraknTestEnv.stopKafka;
import static java.lang.System.currentTimeMillis;
import static java.nio.file.Files.setPosixFilePermissions;
import static java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.GROUP_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_READ;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static java.util.stream.Collectors.joining;

/**
 * Start a SingleQueueEngine from the packaged distribution.
 * This context can be used for integration tests.
 *
 * @author alexandraorth
 */
public class DistributionContext extends ExternalResource {

    private static final FilenameFilter jarFiles = (dir, name) -> name.toLowerCase().endsWith(".jar");
    private static final String ZIP = "grakn-dist-" + GraknVersion.VERSION + ".zip";
    private static final String CURRENT_DIRECTORY = System.getProperty("user.dir");
    private static final String TARGET_DIRECTORY = CURRENT_DIRECTORY + "/../grakn-dist/target/";
    private static final String DIST_DIRECTORY = TARGET_DIRECTORY + "grakn-dist-" + GraknVersion.VERSION;
    private static final Set<PosixFilePermission> permissions =  EnumSet.of(
            OWNER_EXECUTE, OWNER_READ, OWNER_WRITE,
            GROUP_EXECUTE, GROUP_WRITE, GROUP_READ,
            OTHERS_EXECUTE, OTHERS_READ, OTHERS_WRITE);

    private Process engineProcess;
    private int port = 4567;

    private DistributionContext(){}

    public static DistributionContext startSingleQueueEngineProcess(){
        return new DistributionContext();
    }

    public DistributionContext port(int port) {
        this.port = port;
        return this;
    }

    public boolean restart() throws IOException {
        boolean isStarted = engineProcess != null && engineProcess.isAlive();
        if(!isStarted){
            return false;
        }

        engineProcess.destroyForcibly();
        engineProcess = newEngineProcess(port);
        waitForEngine(port);
        return true;
    }

    public int port(){
        return port;
    }

    @Override
    public void before() throws Throwable {
        unzipDistribution();

        startKafka();

        engineProcess = newEngineProcess(port);
        waitForEngine(port);
    }

    @Override
    public void after() {
        engineProcess.destroyForcibly();

        try {
            stopKafka();
        } catch (Exception e) {
            throw new RuntimeException("Could not shut down", e);
        }
    }

    private void unzipDistribution() throws ZipException, IOException {
        // Unzip the distribution
        ZipFile zipped = new ZipFile( TARGET_DIRECTORY + ZIP);
        zipped.extractAll(TARGET_DIRECTORY);

        setPosixFilePermissions(new File(DIST_DIRECTORY + "/bin/grakn-engine.sh").toPath(), permissions);
    }

    private Process newEngineProcess(Integer port) throws IOException {
        // Set correct port & task manager
        Properties properties = GraknEngineConfig.getInstance().getProperties();
        properties.setProperty(LOGGING_LEVEL, "INFO");
        properties.setProperty(SERVER_PORT_NUMBER, port.toString());
        properties.setProperty(TASK_MANAGER_IMPLEMENTATION, SingleQueueTaskManager.class.getName());

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
        return new ProcessBuilder(commands).inheritIO().start();
    }

    /**
     * Get the class path of all the jars in the /lib folder
     */
    private String getClassPath(){
        return Stream.of(new File(DIST_DIRECTORY + "/lib").listFiles(jarFiles))
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
    }
}