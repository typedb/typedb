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

package ai.grakn.engine;

import ai.grakn.GraknSystemProperty;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.GraknVersion;
import com.google.common.base.StandardSystemProperty;
import java.io.FileInputStream;
import java.io.IOException;
import static java.lang.Math.min;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Singleton class used to read config file and make all the settings available to the Grakn Engine classes.
 *
 * @author Marco Scoppetta
 */
public class GraknEngineConfig {

    public static final String GRAKN_VERSION_KEY = "grakn.version";

    public static final String FACTORY_INTERNAL = "factory.internal";
    public static final String FACTORY_ANALYTICS = "factory.analytics";

    public static final String DEFAULT_CONFIG_FILE = "../conf/main/grakn.properties";

    public static final String DEFAULT_KEYSPACE_PROPERTY = "graph.default-keyspace";

    public static final String NUM_THREADS_PROPERTY = "loader.threads";
    public static final String JWT_SECRET_PROPERTY = "JWT.secret";
    public static final String PASSWORD_PROTECTED_PROPERTY = "password.protected";
    public static final String ADMIN_PASSWORD_PROPERTY = "admin.password";

    public static final String SERVER_HOST_NAME = "server.host";
    public static final String SERVER_PORT_NUMBER = "server.port";

    public static final String LOADER_REPEAT_COMMITS = "loader.repeat-commits";

    public static final String REDIS_HOST = "redis.host";
    public static final String REDIS_SENTINEL_HOST = "redis.sentinel.host";
    public static final String REDIS_SENTINEL_MASTER = "redis.sentinel.master";

    public static final String QUEUE_CONSUMERS = "queue.consumers";

    public static final String STATIC_FILES_PATH = "server.static-file-dir";

    // Engine Config
    public static final String TASK_MANAGER_IMPLEMENTATION = "taskmanager.implementation";

    // Delay for the post processing task in milliseconds
    public static final String POST_PROCESSING_TASK_DELAY = "tasks.postprocessing.delay";
    public static final String TASKS_RETRY_DELAY = "tasks.retry.delay";

    public static final int WEBSOCKET_TIMEOUT = 3600000;

    private static String configFilePath = null;

    private static final Logger LOG = LoggerFactory.getLogger(GraknEngineConfig.class);

    private static final int MAX_NUMBER_OF_THREADS = 120;
    private final Properties prop = new Properties();
    private int numOfThreads = -1;

    public static GraknEngineConfig create() {
        setConfigFilePath();
        return GraknEngineConfig.read(getConfigFilePath());
    }

    public static GraknEngineConfig read(String path) {
        return new GraknEngineConfig(path);
    }

    private GraknEngineConfig(String path) {
        String projectPath = getProjectPath();
        setGraknVersion();
        try (FileInputStream inputStream = new FileInputStream(path)){
            prop.load(inputStream);
        } catch (IOException e) {
            LOG.error("Could not load engine properties from {}", path, e);
        }
        computeThreadsNumber();
        LOG.info("Project directory in use: {}", projectPath);
        LOG.info("Configuration file in use: {}", configFilePath);
        LOG.info("Number of threads set to: {}", numOfThreads);
    }

    private void setGraknVersion(){
        prop.setProperty(GRAKN_VERSION_KEY, GraknVersion.VERSION);
    }

    public void setConfigProperty(String key, String value){
        prop.setProperty(key,value);
    }

    /**
     * Check if the JVM argument "-Dgrakn.conf" (which represents the path to the config file to use) is set.
     * If it is not set, it sets it to the default one.
     */
    private static void setConfigFilePath() {
        if (configFilePath != null && !configFilePath.isEmpty()) {
            return;
        }
       configFilePath = (GraknSystemProperty.CONFIGURATION_FILE.value() != null) ? GraknSystemProperty.CONFIGURATION_FILE.value() : GraknEngineConfig.DEFAULT_CONFIG_FILE;
        if (!Paths.get(configFilePath).isAbsolute()) {
            configFilePath = getProjectPath() + configFilePath;
        }
    }

    /**
     * Compute the number of threads available to determine the size of all the Grakn Engine thread pools.
     * If the loader.threads param is set to 0 in the config file, the number of threads will be set
     * equal to the number of available processor to the current JVM.
     */
    private void computeThreadsNumber() {
        numOfThreads = Integer.parseInt(prop.getProperty(NUM_THREADS_PROPERTY));
        if (numOfThreads == 0) {
            int availableProcessors = Runtime.getRuntime().availableProcessors();
            LOG.warn("Number of threads set to 0 in properties. Running with {} processors instead.", availableProcessors);
            numOfThreads = availableProcessors;
        }
        numOfThreads = min(MAX_NUMBER_OF_THREADS, numOfThreads);
    }

    /**
     * @return Number of available threads to be used to instantiate new threadpools.
     */
    public int getAvailableThreads() {
        if (numOfThreads == -1) {
            computeThreadsNumber();
        }
        return numOfThreads;
    }

    /**
     * @param path The name of the property inside the Properties map that refers to a path
     * @return The requested property as a full path. If it is specified as a relative path,
     * this method will return the path prepended with the project path.
     */
    public String getPath(String path) {
        String propertyPath = prop.getProperty(path);
        if (Paths.get(propertyPath).isAbsolute()) {
            return propertyPath;
        }
        return getProjectPath() + propertyPath;
    }

    /**
     * @return The project path. If it is not specified as a JVM parameter it will be set equal to
     * user.dir folder.
     */
    private static String getProjectPath() {
        if (GraknSystemProperty.CURRENT_DIRECTORY.value() == null) {
            GraknSystemProperty.CURRENT_DIRECTORY.set(StandardSystemProperty.USER_DIR.value());
        }
        return GraknSystemProperty.CURRENT_DIRECTORY.value() + "/";
    }

    /**
     * @return The path to the config file currently in use. Default: /conf/main/grakn.properties
     */
    static public String getConfigFilePath() {
        return configFilePath;
    }

    public Properties getProperties() {
        return prop;
    }

    public String getProperty(String property) {
         if(prop.containsKey(property)) {
             return prop.getProperty(property);
         } else {
            throw new RuntimeException(ErrorMessage.UNAVAILABLE_PROPERTY.getMessage(property, configFilePath));
         }
    }

    public Optional<String> tryProperty(String property) {
        return Optional.ofNullable(prop.getProperty(property));
    }

    public int getPropertyAsInt(String property) {
        return Integer.parseInt(getProperty(property));
    }

    public long getPropertyAsLong(String property) {
        return Long.parseLong(getProperty(property));
    }

    public boolean getPropertyAsBool(String property, boolean defaultValue) {
        return prop.containsKey(property) ? Boolean.parseBoolean(prop.getProperty(property))
                                          : defaultValue;
    }

    public String uri() {
        return getProperty(SERVER_HOST_NAME) + ":" + getProperty(SERVER_PORT_NUMBER);
    }

    static List<String> parseCSValue(String s) {
        return Arrays.stream(s.split(",")).map(String::trim).filter(t -> !t.isEmpty()).collect(Collectors.toList());
    }

    static final String GRAKN_ASCII =
                    "     ___  ___  ___  _  __ _  _     ___  ___     %n" +
                    "    / __|| _ \\/   \\| |/ /| \\| |   /   \\|_ _|    %n" +
                    "   | (_ ||   /| - || ' < | .` | _ | - | | |     %n" +
                    "    \\___||_|_\\|_|_||_|\\_\\|_|\\_|(_)|_|_||___|   %n%n" +
                    " Web Dashboard available at [%s]";
}
