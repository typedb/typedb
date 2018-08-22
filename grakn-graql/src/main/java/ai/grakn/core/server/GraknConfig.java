/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package ai.grakn.core.server;

import ai.grakn.GraknConfigKey;
import ai.grakn.GraknSystemProperty;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.SimpleURI;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Collectors;


/**
 * Singleton class used to read config file and make all the settings available to the Grakn Engine classes.
 *
 * @author Marco Scoppetta
 */
public class GraknConfig {

    private final Properties prop;

    /**
     * The path to the config file currently in use. Default: ./conf/main/grakn.properties
     */
    private static final Path DEFAULT_CONFIG_FILE = Paths.get(".", "conf", "main", "grakn.properties");

    public static final Path PROJECT_PATH = CommonUtil.getProjectPath();
    public static final Path CONFIG_FILE_PATH = getConfigFilePath(PROJECT_PATH);

    private static final Logger LOG = LoggerFactory.getLogger(GraknConfig.class);

    protected static final String GRAKN_ASCII = loadGraknAsciiFile(PROJECT_PATH, Paths.get(".","services","grakn-core", "grakn-ascii.txt"));

    private static GraknConfig defaultConfig = null;

    public static GraknConfig empty() {
        return GraknConfig.of(new Properties());
    }

    public static GraknConfig create() {
        if(defaultConfig == null){ defaultConfig = GraknConfig.read(CONFIG_FILE_PATH.toFile()); }
        return defaultConfig;
    }

    public static GraknConfig read(File path) {
        Properties prop = new Properties();
        try (FileInputStream inputStream = new FileInputStream(path)) {
            prop.load(inputStream);
        } catch (IOException e) {
            LOG.error("Could not load engine properties from {}", path, e);
        }
        LOG.info("Project directory in use: {}", PROJECT_PATH);
        LOG.info("Configuration file in use: {}", CONFIG_FILE_PATH);
        return GraknConfig.of(prop);
    }

    public static GraknConfig of(Properties properties) {
        return new GraknConfig(properties);
    }

    private GraknConfig(Properties prop) {
        this.prop = prop;
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

    @JsonValue
    public Properties properties() {
        return prop;
    }

    public <T> T getProperty(GraknConfigKey<T> key) {
        return key.parse(prop.getProperty(key.name()), CONFIG_FILE_PATH);
    }

    public SimpleURI uri() {
        return new SimpleURI(getProperty(GraknConfigKey.SERVER_HOST_NAME), getProperty(GraknConfigKey.SERVER_PORT));
    }

    private static String loadGraknAsciiFile(Path projectPath, Path graknAsciiPath) {
        Path asciiPath = projectPath.resolve(graknAsciiPath);
        try {
            File asciiFile = asciiPath.toFile();
            return Files.readLines(asciiFile, Charsets.UTF_8).stream().collect(Collectors.joining("\n"));
        } catch (IOException e) {
            // couldn't find Grakn ASCII art. Let's just fail gracefully
            LOG.warn("Oops, unable to find Grakn ASCII art. Will just display nothing then.");
            return "";
        }
    }
}
