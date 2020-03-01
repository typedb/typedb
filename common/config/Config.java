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

package grakn.core.common.config;

import grakn.core.common.exception.ErrorMessage;
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


/**
 * Singleton class used to read config file and make all the settings available to the Grakn Server classes.
 *
 */
public class Config {

    private final Properties prop;

    /**
     * The path to the config file currently in use. Default: ./server/conf/grakn.properties
     */
    private static final Path DEFAULT_CONFIG_FILE = Paths.get(".", "server", "conf", "grakn.properties");
    private static final Logger LOG = LoggerFactory.getLogger(Config.class);
    private static Config defaultConfig = null;
    private static final Path PROJECT_PATH = Config.getProjectPath();

    public static final Path CONFIG_FILE_PATH = getConfigFilePath(PROJECT_PATH);


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
    private static Path getProjectPath() {
        if (SystemProperty.CURRENT_DIRECTORY.value() == null) {
            SystemProperty.CURRENT_DIRECTORY.set(System.getProperty("user.dir"));
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

    public Properties properties() {
        return prop;
    }

    public <T> T getProperty(ConfigKey<T> key) {
        String value = prop.getProperty(key.name());

        if (value == null) {
            throw new RuntimeException(ErrorMessage.UNAVAILABLE_PROPERTY.getMessage(key.name(), CONFIG_FILE_PATH));
        }

        return key.parser().read(value);
    }

}
