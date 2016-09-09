/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.engine.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.Properties;

public class ConfigProperties {

    public static final String DEFAULT_CONFIG_FILE = "../conf/main/mindmaps-engine.properties";
    public static final String TEST_CONFIG_FILE = "../conf/test/mindmaps-engine-test.properties";
    public static final String DEFAULT_LOG_CONFIG_FILE = "../conf/main/logback.xml";

    public static final String GRAPH_CONFIG_PROPERTY = "graphdatabase.config";
    public static final String GRAPH_BATCH_CONFIG_PROPERTY = "graphdatabase.batch-config";
    public static final String GRAPH_COMPUTER_CONFIG_PROPERTY = "graphdatabase.computer";

    public static final String DEFAULT_GRAPH_NAME_PROPERTY = "graphdatabase.default-graph-name";

    public static final String BATCH_SIZE_PROPERTY = "blockingLoader.batch-size";

    public static final String NUM_THREADS_PROPERTY = "loader.threads";

    public static final String SERVER_HOST_NAME = "server.host";
    public static final String SERVER_PORT_NUMBER = "server.port";

    public static final String HAL_DEGREE_PROPERTY = "halBuilder.degree";

    public static final String LOADER_REPEAT_COMMITS = "loader.repeat-commits";

    public static final String POSTPROCESSING_DELAY = "backgroundTasks.post-processing-delay";
    public static final String TIME_LAPSE = "backgroundTasks.time-lapse";

    public static final String STATIC_FILES_PATH = "server.static-file-dir";
    public static final String LOGGING_FILE_PATH = "logging.file";

    public static final String PROJECT_VERSION = "project.version";

    public static final String CURRENT_DIR_SYSTEM_PROPERTY = "mindmaps.dir";
    public static final String CONFIG_FILE_SYSTEM_PROPERTY = "mindmaps.conf";
    public static final String LOG_FILE_OUTPUT_SYSTEM_PROPERTY = "mindmaps.log";

    public static final String LOG_FILE_CONFIG_SYSTEM_PROPERTY = "logback.configurationFile";


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

    public int getAvailableThreads() {
        if (numOfThreads == -1)
            computeThreadsNumber();

        return numOfThreads;
    }

    private void computeThreadsNumber() {

        numOfThreads = Integer.parseInt(prop.getProperty(NUM_THREADS_PROPERTY));

        if (numOfThreads == 0)
            numOfThreads = Runtime.getRuntime().availableProcessors();

        if (numOfThreads > MAX_NUMBER_OF_THREADS)
            numOfThreads = MAX_NUMBER_OF_THREADS;

        LOG.info("Number of threads set to [" + numOfThreads + "]");
    }

    public String getPath(String path) {
        String propertyPath = prop.getProperty(path);
        if (Paths.get(propertyPath).isAbsolute())
            return propertyPath;

        return getProjectPath() + propertyPath;
    }

    private static String getProjectPath() {
        return (System.getProperty(CURRENT_DIR_SYSTEM_PROPERTY) != null) ? System.getProperty(CURRENT_DIR_SYSTEM_PROPERTY) + "/" : System.getProperty("user.dir") + "/";
    }

    String getConfigFilePath() {
        if (configFilePath == null) setConfigFilePath();
        return configFilePath;
    }

    private void setConfigFilePath() {
        configFilePath = (System.getProperty(CONFIG_FILE_SYSTEM_PROPERTY) != null) ? System.getProperty(CONFIG_FILE_SYSTEM_PROPERTY) : ConfigProperties.DEFAULT_CONFIG_FILE;
        if (!Paths.get(configFilePath).isAbsolute())
            configFilePath = getProjectPath() + configFilePath;

    }

    private void setLogConfigFile() {
        if (System.getProperty(LOG_FILE_CONFIG_SYSTEM_PROPERTY) == null)
            System.setProperty(LOG_FILE_CONFIG_SYSTEM_PROPERTY, getProjectPath() + DEFAULT_LOG_CONFIG_FILE);

        System.setProperty(LOG_FILE_OUTPUT_SYSTEM_PROPERTY,getPath(LOGGING_FILE_PATH));
    }

    private ConfigProperties() {
        prop = new Properties();
        try {
            prop.load(new FileInputStream(getConfigFilePath()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        setLogConfigFile();
        LOG = LoggerFactory.getLogger(ConfigProperties.class);
        LOG.info("Configuration file in use: [" + configFilePath + "]");

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
}
