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

import ai.grakn.GraknConfigKey;
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

    private static final String DEFAULT_CONFIG_FILE = "../conf/main/grakn.properties";

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
        setConfigProperty(GraknConfigKey.GRAKN_VERSION_KEY, GraknVersion.VERSION);
    }

    public <T> void setConfigProperty(GraknConfigKey<T> key, T value){
        prop.setProperty(key.value(), value.toString());
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
     * @param path A string representing a path
     * @return The requested string as a full path. If it is specified as a relative path,
     * this method will return the path prepended with the project path.
     */
    public static String extractPath(String path) {
        if (Paths.get(path).isAbsolute()) {
            return path;
        }
        return getProjectPath() + path;
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

    public <T> T getProperty(GraknConfigKey<T> key) {
        return tryProperty(key).orElseThrow(() ->
            new RuntimeException(ErrorMessage.UNAVAILABLE_PROPERTY.getMessage(key.value(), configFilePath))
        );
    }

    public <T> Optional<T> tryProperty(GraknConfigKey<T> key) {
        return Optional.ofNullable(prop.getProperty(key.value())).map(key::parse);
    }

    public String uri() {
        return getProperty(GraknConfigKey.SERVER_HOST_NAME) + ":" + getProperty(GraknConfigKey.SERVER_PORT_NUMBER);
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
