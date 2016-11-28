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

package ai.grakn.engine.util;

import ai.grakn.util.ErrorMessage;
import ai.grakn.util.GraknVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.Properties;


/**
 * Singleton class used to read config file and make all the settings available to the Grakn Engine classes.
 */

public class ConfigProperties {

    //Test Configs
    public static final String TEST_CONFIG_FILE = "../conf/test/tinker/grakn-engine.properties";

    public static final String DEFAULT_CONFIG_FILE = "../conf/main/grakn-engine.properties";
    public static final String DEFAULT_LOG_CONFIG_FILE = "../conf/main/logback.xml";

    public static final String GRAPH_CONFIG_PROPERTY = "graphdatabase.config";
    public static final String GRAPH_BATCH_CONFIG_PROPERTY = "graphdatabase.batch-config";
    public static final String GRAPH_COMPUTER_CONFIG_PROPERTY = "graphdatabase.computer";

    public static final String DEFAULT_KEYSPACE_PROPERTY = "graphdatabase.default-keyspace";

    public static final String BATCH_SIZE_PROPERTY = "blockingLoader.batch-size";

    public static final String NUM_THREADS_PROPERTY = "loader.threads";
    public static final String POLLING_FREQUENCY_PROPERTY = "loader.polling-frequency";


    public static final String SERVER_HOST_NAME = "server.host";
    public static final String SERVER_PORT_NUMBER = "server.port";

    public static final String HAL_DEGREE_PROPERTY = "halBuilder.degree";

    public static final String LOADER_REPEAT_COMMITS = "loader.repeat-commits";

    public static final String POSTPROCESSING_DELAY = "backgroundTasks.post-processing-delay";
    public static final String TIME_LAPSE = "backgroundTasks.time-lapse";

    public static final String STATIC_FILES_PATH = "server.static-file-dir";
    public static final String LOGGING_FILE_PATH = "logging.file";
    public static final String LOGGING_LEVEL = "logging.level";


    public static final String PROJECT_VERSION = "project.version";

    public static final String CURRENT_DIR_SYSTEM_PROPERTY = "grakn.dir";
    public static final String CONFIG_FILE_SYSTEM_PROPERTY = "grakn.conf";
    public static final String LOG_FILE_OUTPUT_SYSTEM_PROPERTY = "grakn.log.file";
    public static final String LOG_LEVEL_SYSTEM_PROPERTY = "grakn.log.level";


    public static final String LOG_FILE_CONFIG_SYSTEM_PROPERTY = "logback.configurationFile";

    public static final String SYSTEM_GRAPH_NAME = "graknSystem";
    public static final String SYSTEM_ONTOLOGY_FILE = "system.gql";


    private Logger LOG;

    private final int MAX_NUMBER_OF_THREADS = 120;
    private Properties prop;
    private static ConfigProperties instance = null;
    private String configFilePath = null;
    private int numOfThreads = -1;

    public synchronized static ConfigProperties getInstance() {
        if (instance == null) instance = new ConfigProperties();
        return instance;
    }

    private ConfigProperties() {
        getProjectPath();
        prop = new Properties();
        try {
            prop.load(new FileInputStream(getConfigFilePath()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        prop.put(PROJECT_VERSION, GraknVersion.VERSION);
        initialiseLogger();
        computeThreadsNumber();
        LOG.info("Project directory in use: [" + getProjectPath() + "]");
        LOG.info("Configuration file in use: [" + configFilePath + "]");
        LOG.info("Number of threads set to [" + numOfThreads + "]");
    }


    // Setters


    /**
     * Check if the JVM argument "-Dgrakn.conf" (which represents the path to the config file to use) is set.
     * If it is not set, it sets it to the default one.
     */
    private void setConfigFilePath() {
        configFilePath = (System.getProperty(CONFIG_FILE_SYSTEM_PROPERTY) != null) ? System.getProperty(CONFIG_FILE_SYSTEM_PROPERTY) : ConfigProperties.DEFAULT_CONFIG_FILE;
        if (!Paths.get(configFilePath).isAbsolute())
            configFilePath = getProjectPath() + configFilePath;

    }

    /**
     * Check if the JVM argument "-Dlogback.configurationFile" is set.
     * If it is not set, it sets it to the default one.
     * It also sets the -Dgrakn.log.file system property equal to the one specified in grakn-engine.properties.
     * The grakn.log.file property will be used by logback.xml
     */
    private void initialiseLogger() {
        if (System.getProperty(LOG_FILE_CONFIG_SYSTEM_PROPERTY) == null)
            System.setProperty(LOG_FILE_CONFIG_SYSTEM_PROPERTY, getProjectPath() + DEFAULT_LOG_CONFIG_FILE);

        System.setProperty(LOG_FILE_OUTPUT_SYSTEM_PROPERTY, getPath(LOGGING_FILE_PATH));

        setLogLevel();

        if (!(new File(System.getProperty(LOG_FILE_CONFIG_SYSTEM_PROPERTY))).exists()) {
            LoggerFactory.getLogger(ConfigProperties.class).error(ErrorMessage.NO_LOG_CONFIG_FILE.getMessage(System.getProperty(LOG_FILE_CONFIG_SYSTEM_PROPERTY)));
        } else {
            LOG = LoggerFactory.getLogger(ConfigProperties.class);
            LOG.info("Logging configuration file in use:[" + System.getProperty(LOG_FILE_CONFIG_SYSTEM_PROPERTY) + "]");
        }
    }

    /**
     * Set Grakn logging level.
     * If the -Dgrakn.log.level is set, that value will be used,
     * otherwise it will be used the one specified in the config file.
     */
    private void setLogLevel() {
        if (System.getProperty(LOG_LEVEL_SYSTEM_PROPERTY) == null)
            System.setProperty(LOG_LEVEL_SYSTEM_PROPERTY, prop.getProperty(LOGGING_LEVEL));
    }

    /**
     * Compute the number of threads available to determine the size of all the Grakn Engine thread pools.
     * If the loader.threads param is set to 0 in the config file, the number of threads will be set
     * equal to the number of available processor to the current JVM.
     */
    private void computeThreadsNumber() {

        numOfThreads = Integer.parseInt(prop.getProperty(NUM_THREADS_PROPERTY));

        if (numOfThreads == 0)
            numOfThreads = Runtime.getRuntime().availableProcessors();

        if (numOfThreads > MAX_NUMBER_OF_THREADS)
            numOfThreads = MAX_NUMBER_OF_THREADS;
    }


    // Getters

    /**
     * @return The path to the grakn.log file in use.
     */
    public String getLogFilePath() {
        return System.getProperty(LOG_FILE_OUTPUT_SYSTEM_PROPERTY);
    }

    /**
     * @return Number of available threads to be used to instantiate new threadpools.
     */
    public int getAvailableThreads() {
        if (numOfThreads == -1)
            computeThreadsNumber();

        return numOfThreads;
    }

    /**
     * @param path The name of the property inside the Properties map that refers to a path
     * @return The requested property as a full path. If it is specified as a relative path,
     * this method will return the path prepended with the project path.
     */
    public String getPath(String path) {
        String propertyPath = prop.getProperty(path);
        if (Paths.get(propertyPath).isAbsolute())
            return propertyPath;

        return getProjectPath() + propertyPath;
    }

    /**
     * @return The project path. If it is not specified as a JVM parameter it will be set equal to
     * user.dir folder.
     */
    private static String getProjectPath() {
        if (System.getProperty(CURRENT_DIR_SYSTEM_PROPERTY) == null)
            System.setProperty(CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir"));

        return System.getProperty(CURRENT_DIR_SYSTEM_PROPERTY) + "/";
    }

    /**
     * @return The path to the config file currently in use. Default: /conf/main/grakn-engine.properties
     */
    String getConfigFilePath() {
        if (configFilePath == null) setConfigFilePath();
        return configFilePath;
    }

    public Properties getProperties() {
        return prop;
    }

    public String getProperty(String property) {
        return prop.getProperty(property);
    }

    public int getPropertyAsInt(String property) {
        return Integer.parseInt(prop.getProperty(property));
    }

    public long getPropertyAsLong(String property) {
        return Long.parseLong(prop.getProperty(property));
    }

    public static final String GRAKN_ASCII =
                      "     ___  ___  ___  _  __ _  _     ___  ___     \n" +
                    "    / __|| _ \\/   \\| |/ /| \\| |   /   \\|_ _|    \n" +
                    "   | (_ ||   /| - || ' < | .` | _ | - | | |     \n" +
                    "    \\___||_|_\\|_|_||_|\\_\\|_|\\_|(_)|_|_||___|   \n\n" +
                      " Web Dashboard available at [%s]";

}
