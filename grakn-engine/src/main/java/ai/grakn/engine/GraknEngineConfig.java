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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
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

    /**
     * The path to the config file currently in use. Default: ../conf/main/grakn.properties
     */
    private static final Path DEFAULT_CONFIG_FILE = Paths.get("..", "conf", "main", "grakn.properties");

    public static final int WEBSOCKET_TIMEOUT = 3600000;

    public static final Path CONFIG_FILE_PATH = getConfigFilePath();
    public static final Path PROJECT_PATH = getProjectPath();

    private static final Logger LOG = LoggerFactory.getLogger(GraknEngineConfig.class);

    private final Properties prop = new Properties();

    protected static final String GRAKN_ASCII = loadGraknAsciiFile();

    private static final Path GRAKN_ASCII_PATH = Paths.get("grakn", "grakn-ascii.txt");

    public static GraknEngineConfig create() {
        return GraknEngineConfig.read(CONFIG_FILE_PATH.toFile());
    }

    public static GraknEngineConfig read(File path) {
        return new GraknEngineConfig(path);
    }

    private GraknEngineConfig(File path) {
        setConfigProperty(GraknConfigKey.VERSION, GraknVersion.VERSION);
        try (FileInputStream inputStream = new FileInputStream(path)) {
            prop.load(inputStream);
        } catch (IOException e) {
            LOG.error("Could not load engine properties from {}", path, e);
        }
        LOG.info("Project directory in use: {}", PROJECT_PATH);
        LOG.info("Configuration file in use: {}", CONFIG_FILE_PATH);
    }

    public void write(File path) throws IOException {
        try(FileOutputStream os = new FileOutputStream(path)) {
            prop.store(os, null);
        }
    }

    public <T> void setConfigProperty(GraknConfigKey<T> key, T value) {
        prop.setProperty(key.value(), value.toString());
    }

    /**
     * Check if the JVM argument "-Dgrakn.conf" (which represents the path to the config file to use) is set.
     * If it is not set, it sets it to the default one.
     */
    private static Path getConfigFilePath() {
        String pathString = GraknSystemProperty.CONFIGURATION_FILE.value();
        Path path;
        if (pathString == null) {
            path = DEFAULT_CONFIG_FILE;
        } else {
            path = Paths.get(pathString);
        }
        return PROJECT_PATH.resolve(path);
    }

    /**
     * @param path A string representing a path
     * @return The requested string as a full path. If it is specified as a relative path,
     * this method will return the path prepended with the project path.
     */
    public static Path extractPath(Path path) {
        return PROJECT_PATH.resolve(path);
    }

    /**
     * @return The project path. If it is not specified as a JVM parameter it will be set equal to
     * user.dir folder.
     */
    private static Path getProjectPath() {
        if (GraknSystemProperty.CURRENT_DIRECTORY.value() == null) {
            GraknSystemProperty.CURRENT_DIRECTORY.set(StandardSystemProperty.USER_DIR.value());
        }
        return Paths.get(GraknSystemProperty.CURRENT_DIRECTORY.value());
    }

    public Properties getProperties() {
        return prop;
    }

    public <T> T getProperty(GraknConfigKey<T> key) {
        return tryProperty(key).orElseThrow(() ->
                new RuntimeException(ErrorMessage.UNAVAILABLE_PROPERTY.getMessage(key.value(), CONFIG_FILE_PATH))
        );
    }

    public <T> Optional<T> tryProperty(GraknConfigKey<T> key) {
        return Optional.ofNullable(prop.getProperty(key.value())).map(key::parse);
    }

    public String uri() {
        return getProperty(GraknConfigKey.SERVER_HOST_NAME) + ":" + getProperty(GraknConfigKey.SERVER_PORT);
    }

    static List<String> parseCSValue(String s) {
        return Arrays.stream(s.split(",")).map(String::trim).filter(t -> !t.isEmpty()).collect(Collectors.toList());
    }

    private static String loadGraknAsciiFile() {
        Path asciiPath = PROJECT_PATH.resolve(GRAKN_ASCII_PATH);
        try {
            File asciiFile = asciiPath.toFile();
            return FileUtils.readFileToString(asciiFile);
        } catch (IOException e) {
            // couldn't find Grakn ASCII art. Let's just fail gracefully
            LOG.warn("Oops, unable to find Grakn ASCII art. Will just display nothing then.");
            return "";
        }
    }
}
