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
import ai.grakn.util.CommonUtil;
import ai.grakn.util.GraknVersion;
import ai.grakn.util.SimpleURI;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;


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

    public static final Path PROJECT_PATH = CommonUtil.getProjectPath();
    public static final Path CONFIG_FILE_PATH = getConfigFilePath(PROJECT_PATH);

    private static final Logger LOG = LoggerFactory.getLogger(GraknEngineConfig.class);

    private final Properties prop = new Properties();

    protected static final String GRAKN_ASCII = loadGraknAsciiFile(PROJECT_PATH, Paths.get("grakn", "grakn-ascii.txt"));

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
        prop.setProperty(key.name(), key.valueToString(value));
    }

    /**
     * Check if the JVM argument "-Dgrakn.conf" (which represents the path to the config file to use) is set.
     * If it is not set, it sets it to the default one.
     */
    private static Path getConfigFilePath(Path projectPath) {
        String pathString = GraknSystemProperty.CONFIGURATION_FILE.value();
        Path path;
        if (pathString == null) {
            path = DEFAULT_CONFIG_FILE;
        } else {
            path = Paths.get(pathString);
        }
        return projectPath.resolve(path);
    }

    /**
     * @param pathKey A config key for a path
     * @return The requested string as a full path. If it is specified as a relative path,
     * this method will return the path prepended with the project path.
     */
    public Path getPath(GraknConfigKey<Path> pathKey) {
        return PROJECT_PATH.resolve(getProperty(pathKey));
    }

    public Properties getProperties() {
        return prop;
    }

    public <T> T getProperty(GraknConfigKey<T> key) {
        return key.parse(Optional.ofNullable(prop.getProperty(key.name())), CONFIG_FILE_PATH);
    }

    public SimpleURI uri() {
        return new SimpleURI(getProperty(GraknConfigKey.SERVER_HOST_NAME), getProperty(GraknConfigKey.SERVER_PORT));
    }

    private static String loadGraknAsciiFile(Path projectPath, Path graknAsciiPath) {
        Path asciiPath = projectPath.resolve(graknAsciiPath);
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
