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
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;


/**
 * Singleton class used to read config file and make all the settings available to the Grakn Engine classes.
 *
 * @author Marco Scoppetta
 */
public class GraknEngineConfig {

    public static final String GRAKN_VERSION_KEY = "grakn.version";

    public static final String DEFAULT_CONFIG_FILE = "../conf/main/grakn.properties";

    public static final String WEBSERVER_THREADS = "webserver.threads";

    public static final String SERVER_HOST_NAME = "server.host";
    public static final String SERVER_PORT_NUMBER = "server.port";

    public static final String LOADER_REPEAT_COMMITS = "loader.repeat-commits";

    public static final String REDIS_HOST = "queue.host";
    public static final String REDIS_SENTINEL_HOST = "redis.sentinel.host";
    public static final String REDIS_SENTINEL_MASTER = "redis.sentinel.master";
    public static final String REDIS_POOL_SIZE = "redis.pool-size";

    public static final String QUEUE_CONSUMERS = "queue.consumers";

    public static final String STATIC_FILES_PATH = "server.static-file-dir";

    // Delay for the post processing task in milliseconds
    public static final String POST_PROCESSING_TASK_DELAY = "tasks.postprocessing.delay";
    public static final String TASKS_RETRY_DELAY = "tasks.retry.delay";

    public static final int WEBSOCKET_TIMEOUT = 3600000;

    private static String configFilePath = null;

    private static final Logger LOG = LoggerFactory.getLogger(GraknEngineConfig.class);

    private final Properties prop = new Properties();

    protected static final String GRAKN_ASCII = loadGraknAsciiFile();

    private static final String GRAKN_ASCII_PATH = "/grakn/grakn-ascii.txt";

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
        LOG.info("Project directory in use: {}", projectPath);
        LOG.info("Configuration file in use: {}", configFilePath);
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

    public int tryIntProperty(String property, int defaultValue) {
        return Optional.ofNullable(prop.getProperty(property)).map(Integer::parseInt).orElse(defaultValue);
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

    private static String loadGraknAsciiFile() {
        String asciiPath = getProjectPath() + GRAKN_ASCII_PATH;
        try {
            File asciiFile = Paths.get(asciiPath).toFile();
            return FileUtils.readFileToString(asciiFile);
        } catch (IOException e) {
            // couldn't find Grakn ASCII art. Let's just fail gracefully
            LOG.warn("Oops, unable to find Grakn ASCII art. Will just display nothing then.");
            return "";
        }
    }
}
