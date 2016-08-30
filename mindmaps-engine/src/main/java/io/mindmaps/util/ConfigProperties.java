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

package io.mindmaps.util;

import java.io.FileInputStream;
import java.util.Properties;

public class ConfigProperties {

    public static final String CONFIG_FILE = "../conf/mindmaps-engine.properties";
    public static final String TEST_CONFIG_FILE = "../conf/mindmaps-engine-test.properties";

    public static final String GRAPH_CONFIG_PROPERTY = "graphdatabase.config";
    public static final String GRAPH_BATCH_CONFIG_PROPERTY = "graphdatabase.batch-config";
    public static final String GRAPH_COMPUTER_CONFIG_PROPERTY = "graphdatabase.computer";

    public static final String DEFAULT_GRAPH_NAME_PROPERTY = "graphdatabase.default-graph-name";

    public static final String BATCH_SIZE_PROPERTY = "blockingLoader.batch-size";
    public static final String NUM_THREADS_PROPERTY = "blockingLoader.num-threads";

    public static final String SERVER_HOST_NAME = "server.host";
    public static final String SERVER_PORT_NUMBER = "server.port";

    public static final String HAL_DEGREE_PROPERTY = "halBuilder.degree";

    public static final String LOADER_REPEAT_COMMITS = "loader.repeat-commits";

    public static final String MAINTENANCE_ITERATION = "backgroundTasks.maintenance-iteration";

    public static final String LOGGING_FILE_PATH = "logging.file";
    public static final String DATE_FORMAT = "yyyy/MM/dd HH:mm:ss";
    public static final String STATIC_FILES_PATH = "server.static-file-dir";

    public static final String CURRENT_DIR_SYSTEM_PROPERTY = "mindmaps.dir";
    public static final String CONFIG_FILE_SYSTEM_PROPERTY = "mindmaps.conf";


    private Properties prop;
    private static ConfigProperties instance = null;

    public synchronized static ConfigProperties getInstance() {
        if (instance == null) instance = new ConfigProperties();
        return instance;
    }


    public static String getProjectPath() {
        return (System.getProperty(CURRENT_DIR_SYSTEM_PROPERTY) != null) ? System.getProperty(CURRENT_DIR_SYSTEM_PROPERTY) + "/" : System.getProperty("user.dir") + "/";
    }

    public String getConfigFilePath() {
        return (System.getProperty(CONFIG_FILE_SYSTEM_PROPERTY) != null) ? System.getProperty(CONFIG_FILE_SYSTEM_PROPERTY) : ConfigProperties.CONFIG_FILE;
    }

    private ConfigProperties() {
        prop = new Properties();
        try {
            prop.load(new FileInputStream(getProjectPath() + getConfigFilePath()));
        } catch (Exception e) {
            e.printStackTrace();
        }
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
}
