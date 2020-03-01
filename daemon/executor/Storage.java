/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.daemon.executor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import grakn.core.common.config.Config;
import grakn.core.common.config.ConfigKey;
import grakn.core.common.config.SystemProperty;
import grakn.core.daemon.exception.GraknDaemonException;
import grakn.core.server.GraknStorage;
import org.apache.cassandra.tools.NodeTool;
import org.apache.commons.io.FileUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static grakn.core.daemon.executor.Executor.WAIT_INTERVAL_SECOND;

/**
 * A class responsible for managing the bootup-related process for the Storage component, including
 * starting and stopping, performing status checks, and cleaning the data.
 * The PID file for the Storage component is managed internally by Cassandra and not by this class. This means that
 * you will not find any code which creates or deletes the PID file for the Storage component.
 */
public class Storage {

    private static final String DISPLAY_NAME = "Storage";
    private static final long STORAGE_STARTUP_TIMEOUT_SECOND = 60;
    private static final Path STORAGE_PIDFILE = Paths.get(System.getProperty("java.io.tmpdir"), "grakn-storage.pid");
    private static final String JAVA_OPTS = SystemProperty.STORAGE_JAVAOPTS.value();

    private static final String EMPTY_VALUE = "";
    private static final String CONFIG_PARAM_PREFIX = "storage.internal.";
    private static final String SAVED_CACHES_SUBDIR = "cassandra/saved_caches";
    private static final String COMMITLOG_SUBDIR = "cassandra/commitlog";
    private static final String DATA_SUBDIR = "cassandra/data";
    private static final String DATA_FILE_DIR_CONFIG_KEY = "data_file_directories";
    private static final String SAVED_CACHES_DIR_CONFIG_KEY = "saved_caches_directory";
    private static final String COMMITLOG_DIR_CONFIG_KEY = "commitlog_directory";
    private static final String STORAGE_CONFIG_PATH = "server/services/cassandra/";
    private static final String STORAGE_CONFIG_NAME = "cassandra.yaml";


    private Executor daemonExecutor;
    private final Path graknHome;
    private final Config graknProperties;

    public Storage(Executor processExecutor, Path graknHome, Path graknPropertiesPath) {
        this.graknHome = graknHome;
        this.graknProperties = Config.read(graknPropertiesPath);
        this.daemonExecutor = processExecutor;
    }

    private void initialiseConfig() {
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
            TypeReference<Map<String, Object>> reference = new TypeReference<Map<String, Object>>() {
            };
            ByteArrayOutputStream outputstream = new ByteArrayOutputStream();

            // Read the original Cassandra config from services/cassandra/cassandra.yaml into a String
            byte[] oldConfigBytes = Files.readAllBytes(graknHome.resolve(STORAGE_CONFIG_PATH).resolve(STORAGE_CONFIG_NAME));
            String oldConfig = new String(oldConfigBytes, StandardCharsets.UTF_8);

            // Convert the String of config values into a Map
            Map<String, Object> oldConfigMap = mapper.readValue(oldConfig, reference);

            // Set the original config as the starting point of the new config values (replacing null with empty string)
            Map<String, Object> newConfigMap = new HashMap<>();
            oldConfigMap.forEach((key, value) -> {
                newConfigMap.put(key, value == null ? EMPTY_VALUE : value);
            });

            // Read the Grakn config which is available to the user
            Config inputConfig = Config.read(Paths.get(Objects.requireNonNull(SystemProperty.CONFIGURATION_FILE.value())));

            // Set the new data directories for Cassandra
            String newDataDir = inputConfig.getProperty(ConfigKey.DATA_DIR);
            newConfigMap.put(DATA_FILE_DIR_CONFIG_KEY, Collections.singletonList(newDataDir + DATA_SUBDIR));
            newConfigMap.put(SAVED_CACHES_DIR_CONFIG_KEY, newDataDir + SAVED_CACHES_SUBDIR);
            newConfigMap.put(COMMITLOG_DIR_CONFIG_KEY, newDataDir + COMMITLOG_SUBDIR);

            // Overwrite Cassandra config values with values provided in the Grakn config
            inputConfig.properties().stringPropertyNames().stream()
                    .filter(key -> key.contains(CONFIG_PARAM_PREFIX))
                    .forEach(key -> newConfigMap.put(
                            key.replaceAll(CONFIG_PARAM_PREFIX, ""),
                            inputConfig.properties().getProperty(key)
                    ));

            // Write the new Cassandra config into the original file: services/cassandra/cassandra.yaml
            mapper.writeValue(outputstream, newConfigMap);
            String newConfigStr = outputstream.toString(StandardCharsets.UTF_8.name());
            Files.write(graknHome.resolve(STORAGE_CONFIG_PATH).resolve(STORAGE_CONFIG_NAME), newConfigStr.getBytes(StandardCharsets.UTF_8));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Attempt to start Storage if it is not already running
     */
    public void startIfNotRunning() {
        boolean isProcessRunning = daemonExecutor.isProcessRunning(STORAGE_PIDFILE);
        boolean isGraknStorageProcess = daemonExecutor.isAGraknProcess(STORAGE_PIDFILE, GraknStorage.class.getName());
        if (isProcessRunning && isGraknStorageProcess) {
            System.out.println(DISPLAY_NAME + " is already running");
        } else {
            FileUtils.deleteQuietly(STORAGE_PIDFILE.toFile()); // delete dangling STORAGE_PIDFILE, if any
            start();
        }
    }

    public void stop() {
        daemonExecutor.stopProcessIfRunning(STORAGE_PIDFILE, DISPLAY_NAME);
    }

    public void status() {
        daemonExecutor.processStatus(STORAGE_PIDFILE, DISPLAY_NAME, GraknStorage.class.getName());
    }

    public void clean() {
        Path dataDir = Paths.get(graknProperties.getProperty(ConfigKey.DATA_DIR));
        dataDir = dataDir.isAbsolute() ? dataDir : graknHome.resolve(dataDir);
        System.out.print("Cleaning " + DISPLAY_NAME + "...");
        System.out.flush();
        try (Stream<Path> files = Files.walk(dataDir)) {
            files.map(Path::toFile)
                    .sorted(Comparator.comparing(File::isDirectory))
                    .forEach(File::delete);
            Files.createDirectories(dataDir.resolve(DATA_SUBDIR));
            Files.createDirectories(dataDir.resolve(SAVED_CACHES_SUBDIR));
            Files.createDirectories(dataDir.resolve(COMMITLOG_SUBDIR));
            System.out.println("SUCCESS");
        } catch (IOException e) {
            System.out.println("FAILED!");
            System.out.println("Unable to clean " + DISPLAY_NAME);
        }
    }

    public boolean isRunning() {
        return daemonExecutor.isProcessRunning(STORAGE_PIDFILE);
    }

    /**
     * Attempt to start Storage and perform periodic polling until it is ready. The readiness check is performed with nodetool.
     * <p>
     * A GraknDaemonException will be thrown if Storage does not start after a timeout specified
     * in the 'WAIT_INTERVAL_SECOND' field.
     *
     * @throws GraknDaemonException
     */
    private void start() {
        System.out.print("Starting " + DISPLAY_NAME + "...");
        System.out.flush();

        // Consume configuration from Grakn config file into Cassandra config file
        initialiseConfig();

        Future<Executor.Result> result = daemonExecutor.executeAsync(storageCommand(), graknHome.toFile());

        LocalDateTime timeout = LocalDateTime.now().plusSeconds(STORAGE_STARTUP_TIMEOUT_SECOND);

        while (LocalDateTime.now().isBefore(timeout) && !result.isDone()) {
            System.out.print(".");
            System.out.flush();

            if (storageStatus().equals("running")) {
                System.out.println("SUCCESS");
                return;
            }

            try {
                Thread.sleep(WAIT_INTERVAL_SECOND * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }


        try {
            System.out.println("FAILED!");
            System.err.println("Unable to start " + DISPLAY_NAME + ".");
            String errorMessage = "Process exited with code '" + result.get().exitCode() + "': '" + result.get().stderr() + "'";
            System.err.println(errorMessage);
            throw new GraknDaemonException(errorMessage);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GraknDaemonException(e.getMessage(), e);
        } catch (ExecutionException e) {
            throw new GraknDaemonException(e.getMessage(), e);
        }
    }

    private String storageStatus() {
        return daemonExecutor.executeAndWait(nodetoolCommand(), graknHome.toFile()).stdout().trim();
    }

    private List<String> storageCommand() {
        Path logback = graknHome.resolve("services").resolve("cassandra").resolve("logback.xml");
        ArrayList<String> storageCommand = new ArrayList<>();
        storageCommand.add("java");
        storageCommand.add("-cp");
        storageCommand.add(getStorageClassPath());
        storageCommand.add("-Dlogback.configurationFile=" + logback);
        storageCommand.add("-Dcassandra.logdir=" + getStorageLogPathFromGraknProperties());
        storageCommand.add("-Dcassandra-pidfile=" + STORAGE_PIDFILE.toString());
        //default port over for JMX connections, needed for nodetool status
        storageCommand.add("-Dcassandra.jmx.local.port=7199");
        // stop the jvm on OutOfMemoryError as it can result in some data corruption
        storageCommand.add("-XX:+CrashOnOutOfMemoryError");
        if (JAVA_OPTS != null && JAVA_OPTS.length() > 0) {
            storageCommand.addAll(Arrays.asList(JAVA_OPTS.split(" ")));
        }
        storageCommand.add(GraknStorage.class.getCanonicalName());
        return storageCommand;
    }

    private String getStorageClassPath() {
        return graknHome.resolve("server").resolve("services").resolve("lib").toString() + File.separator + "*"
                + File.pathSeparator + graknHome.resolve("server").resolve("services").resolve("cassandra");
    }


    private List<String> nodetoolCommand() {
        Path logback = graknHome.resolve("server").resolve("services").resolve("cassandra").resolve("logback.xml");
        String classpath = graknHome.resolve("server").resolve("services").resolve("lib").toString() + File.separator + "*";
        return Arrays.asList(
                "java", "-cp", classpath,
                "-Dlogback.configurationFile=" + logback,
                NodeTool.class.getCanonicalName(),
                "statusbinary"
        );
    }

    private Path getStorageLogPathFromGraknProperties() {
        Path logPath = Paths.get(graknProperties.getProperty(ConfigKey.LOG_DIR));
        return logPath.isAbsolute() ? logPath : graknHome.resolve(logPath);
    }

}
