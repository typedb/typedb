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

import ai.grakn.util.ErrorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;


/**
 * Singleton class used to read config file and make all the settings available to the Grakn Engine classes.
 *
 * @author Marco Scoppetta
 */
public class GraknEngineConfig {

    public static final String FACTORY_INTERNAL = "factory.internal";
    public static final String FACTORY_ANALYTICS = "factory.analytics";

    public static final String DEFAULT_CONFIG_FILE = "../conf/main/grakn.properties";
    public static final String DEFAULT_LOG_CONFIG_FILE = "../conf/main/logback.xml";

    public static final String DEFAULT_KEYSPACE_PROPERTY = "graphdatabase.default-keyspace";

    public static final String NUM_THREADS_PROPERTY = "loader.threads";
    public static final String JWT_SECRET_PROPERTY = "JWT.secret";
    public static final String PASSWORD_PROTECTED_PROPERTY="password.protected";

    public static final String SERVER_HOST_NAME = "server.host";
    public static final String SERVER_PORT_NUMBER = "server.port";

    public static final String LOADER_REPEAT_COMMITS = "loader.repeat-commits";
    public static final String POST_PROCESSING_DELAY = "backgroundTasks.post-processing-delay";
    public static final String TIME_LAPSE = "backgroundTasks.time-lapse";

    public static final String STATIC_FILES_PATH = "server.static-file-dir";
    public static final String LOGGING_FILE_PATH_MAIN = "logging.file.main";
    public static final String LOGGING_LEVEL = "logging.level";

    public static final String CURRENT_DIR_SYSTEM_PROPERTY = "grakn.dir";
    public static final String CONFIG_FILE_SYSTEM_PROPERTY = "grakn.conf";
    public static final String LOG_FILE_OUTPUT_SYSTEM_PROPERTY_MAIN = "grakn.log.file.main";
    public static final String LOG_LEVEL_SYSTEM_PROPERTY = "grakn.log.level";

    //Post Processing Logging
    public static final String LOG_NAME_POSTPROCESSING_PROPERTY = "grakn.log.name.postprocessing";
    public static final String LOG_NAME_POSTPROCESSING_DEFAULT = "post-processing";
    public static final String LOG_FILE_OUTPUT_SYSTEM_PROPERTY_POST_PROCESSING = "grakn.log.file.postprocessing";
    public static final String LOGGING_FILE_PATH_POST_PROCESSING = "logging.file.postprocessing";

    public static final String POST_PROCESSING_THREADS = "postprocessing.threads";

    public static final String LOG_FILE_CONFIG_SYSTEM_PROPERTY = "logback.configurationFile";

    // Engine Config
    public static final String TASK_MANAGER_IMPLEMENTATION = "taskmanager.implementation";
    public static final String USE_ZOOKEEPER_STORAGE = "taskmanager.storage.zk";

    public static final String ZK_SERVERS = "tasks.zookeeper.servers";
    public static final String ZK_SESSION_TIMEOUT = "tasks.zookeeper.session_timeout_ms";
    public static final String ZK_CONNECTION_TIMEOUT = "tasks.zookeeper.connection_timeout_ms";
    public static final String ZK_BACKOFF_BASE_SLEEP_TIME = "tasks.zookeeper.backoff.base_sleep";
    public static final String ZK_BACKOFF_MAX_RETRIES = "tasks.zookeeper.backoff.max_retries";

    public static final String TASKRUNNER_POLLING_FREQ = "tasks.runner.polling-frequency";

    private Logger LOG;

    private final int MAX_NUMBER_OF_THREADS = 120;
    private final Properties prop;
    private static GraknEngineConfig instance = null;
    private String configFilePath = null;
    private int numOfThreads = -1;

    public synchronized static GraknEngineConfig getInstance() {
        if (instance == null) instance = new GraknEngineConfig();
        return instance;
    }

    private GraknEngineConfig() {
        getProjectPath();
        prop = new Properties();
        try (FileInputStream inputStream = new FileInputStream(getConfigFilePath())){
            prop.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        initialiseLogger();
        computeThreadsNumber();
        LOG.info("Project directory in use: [" + getProjectPath() + "]");
        LOG.info("Configuration file in use: [" + configFilePath + "]");
        LOG.info("Number of threads set to [" + numOfThreads + "]");
    }

    public void setConfigProperty(String key, String value){
        prop.setProperty(key,value);
    }

    /**
     * Check if the JVM argument "-Dgrakn.conf" (which represents the path to the config file to use) is set.
     * If it is not set, it sets it to the default one.
     */
    private void setConfigFilePath() {
        configFilePath = (System.getProperty(CONFIG_FILE_SYSTEM_PROPERTY) != null) ? System.getProperty(CONFIG_FILE_SYSTEM_PROPERTY) : GraknEngineConfig.DEFAULT_CONFIG_FILE;
        if (!Paths.get(configFilePath).isAbsolute()) {
            configFilePath = getProjectPath() + configFilePath;
        }

    }

    /**
     * Check if the JVM argument "-Dlogback.configurationFile" is set.
     * If it is not set, it sets it to the default one.
     * It also sets the -Dgrakn.log.file system property equal to the one specified in grakn.properties.
     * The grakn.log.file property will be used by logback.xml
     */
    private void initialiseLogger() {
        if (System.getProperty(LOG_FILE_CONFIG_SYSTEM_PROPERTY) == null) {
            System.setProperty(LOG_FILE_CONFIG_SYSTEM_PROPERTY, getProjectPath() + DEFAULT_LOG_CONFIG_FILE);
        }

        System.setProperty(LOG_FILE_OUTPUT_SYSTEM_PROPERTY_MAIN, getPath(LOGGING_FILE_PATH_MAIN));
        System.setProperty(LOG_FILE_OUTPUT_SYSTEM_PROPERTY_POST_PROCESSING, getPath(LOGGING_FILE_PATH_POST_PROCESSING));
        System.setProperty(LOG_NAME_POSTPROCESSING_PROPERTY, LOG_NAME_POSTPROCESSING_DEFAULT);

        setLogLevel();

        if (!(new File(System.getProperty(LOG_FILE_CONFIG_SYSTEM_PROPERTY))).exists()) {
            LoggerFactory.getLogger(GraknEngineConfig.class).error(ErrorMessage.NO_LOG_CONFIG_FILE.getMessage(System.getProperty(LOG_FILE_CONFIG_SYSTEM_PROPERTY)));
        } else {
            LOG = LoggerFactory.getLogger(GraknEngineConfig.class);
            LOG.info("Logging configuration file in use:[" + System.getProperty(LOG_FILE_CONFIG_SYSTEM_PROPERTY) + "]");
        }
    }

    /**
     * Set Grakn logging level.
     * If the -Dgrakn.log.level is set, that value will be used,
     * otherwise it will be used the one specified in the config file.
     */
    private void setLogLevel() {
        if (System.getProperty(LOG_LEVEL_SYSTEM_PROPERTY) == null) {
            System.setProperty(LOG_LEVEL_SYSTEM_PROPERTY, prop.getProperty(LOGGING_LEVEL));
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
            numOfThreads = Runtime.getRuntime().availableProcessors();
        }

        if (numOfThreads > MAX_NUMBER_OF_THREADS) {
            numOfThreads = MAX_NUMBER_OF_THREADS;
        }
    }


    // Getters

    /**
     * @return The path to the grakn.log file in use.
     */
    public String getLogFilePath() {
        return System.getProperty(LOG_FILE_OUTPUT_SYSTEM_PROPERTY_MAIN);
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
        if (System.getProperty(CURRENT_DIR_SYSTEM_PROPERTY) == null) {
            System.setProperty(CURRENT_DIR_SYSTEM_PROPERTY, System.getProperty("user.dir"));
        }

        return System.getProperty(CURRENT_DIR_SYSTEM_PROPERTY) + "/";
    }

    /**
     * @return The path to the config file currently in use. Default: /conf/main/grakn.properties
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

    public String getProperty(String property, String defaultValue) {
        String res = prop.getProperty(property);
        if(res != null) {
            return res;
        }

        return defaultValue;
    }

    public int getPropertyAsInt(String property) {
        return Integer.parseInt(prop.getProperty(property));
    }

    public long getPropertyAsLong(String property) {
        return Long.parseLong(prop.getProperty(property));
    }

    public boolean getPropertyAsBool(String property) {
        return Boolean.parseBoolean(prop.getProperty(property));
    }


    public static final String GRAKN_ASCII =
                      "     ___  ___  ___  _  __ _  _     ___  ___     %n" +
                    "    / __|| _ \\/   \\| |/ /| \\| |   /   \\|_ _|    %n" +
                    "   | (_ ||   /| - || ' < | .` | _ | - | | |     %n" +
                    "    \\___||_|_\\|_|_||_|\\_\\|_|\\_|(_)|_|_||___|   %n%n" +
                      " Web Dashboard available at [%s]";

}
