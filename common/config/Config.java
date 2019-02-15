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

package grakn.core.common.config;

import com.google.common.base.Charsets;
import com.google.common.base.StandardSystemProperty;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Collectors;


/**
 * Singleton class used to read config file and make all the settings available to the Grakn Server classes.
 *
 */
public class Config {

    private final Properties prop;

    /**
     * The path to the config file currently in use. Default: ./conf/grakn.properties
     */
    private static final Path DEFAULT_CONFIG_FILE = Paths.get(".", "conf", "grakn.properties");
    private static final Logger LOG = LoggerFactory.getLogger(Config.class);
    private static Config defaultConfig = null;

    public static final Path PROJECT_PATH = Config.getProjectPath();
    public static final Path CONFIG_FILE_PATH = getConfigFilePath(PROJECT_PATH);
    public static final String GRAKN_ASCII = loadGraknAsciiFile(PROJECT_PATH, Paths.get(".", "services", "grakn", "grakn-core-ascii.txt"));


    public static Config create() {
        if(defaultConfig == null){
            defaultConfig = Config.read(CONFIG_FILE_PATH);
        }
        return defaultConfig;
    }

    /**
     * @return The project path. If it is not specified as a JVM parameter it will be set equal to
     * user.dir folder.
     */
    public static Path getProjectPath() {
        if (SystemProperty.CURRENT_DIRECTORY.value() == null) {
            SystemProperty.CURRENT_DIRECTORY.set(StandardSystemProperty.USER_DIR.value());
        }
        return Paths.get(SystemProperty.CURRENT_DIRECTORY.value());
    }

    public static Config read(Path path) {
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(path.toString());
        } catch (FileNotFoundException e) {
            LOG.error("Could not load server properties from {}", path, e);
        }
        return read(inputStream);
    }

    public static Config read(InputStream inputStream){
        Properties prop = new Properties();
        try {
            prop.load(inputStream);
        } catch (IOException e) {
            LOG.error("Could not load server properties from input stream provided", e);
        }
        return Config.of(prop);
    }

    public static Config of(Properties properties) {
        Properties localProps = new Properties();
        properties.forEach((key, value)-> localProps.setProperty((String)key, (String)value));
        return new Config(localProps);
    }

    private Config(Properties prop) {
        this.prop = prop;
    }

    public void write(File path) throws IOException {
        try(FileOutputStream os = new FileOutputStream(path)) {
            prop.store(os, null);
        }
    }

    public <T> void setConfigProperty(ConfigKey<T> key, T value) {
        prop.setProperty(key.name(), key.valueToString(value));
    }

    /**
     * Check if the JVM argument "-Dgrakn.conf" (which represents the path to the config file to use) is set.
     * If it is not set, it sets it to the default one.
     */
    private static Path getConfigFilePath(Path projectPath) {
        String pathString = SystemProperty.CONFIGURATION_FILE.value();
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
    public Path getPath(ConfigKey<Path> pathKey) {
        return PROJECT_PATH.resolve(getProperty(pathKey));
    }

    public Properties properties() {
        return prop;
    }

    public <T> T getProperty(ConfigKey<T> key) {
        return key.parse(prop.getProperty(key.name()), CONFIG_FILE_PATH);
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
