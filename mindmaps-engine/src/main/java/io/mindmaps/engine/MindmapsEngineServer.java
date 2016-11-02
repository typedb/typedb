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
package io.mindmaps.engine;


import io.mindmaps.engine.controller.*;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.exception.MindmapsEngineServerException;
import io.mindmaps.util.REST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLConnection;

import static spark.Spark.*;

/**
 * Main class in charge to start a web server and all the REST controllers.
 */

public class MindmapsEngineServer {

    private static ConfigProperties prop = ConfigProperties.getInstance();
    private static Logger LOG = null;

    public static void main(String[] args) {
        start();
    }

    public static void start() {

        LOG = LoggerFactory.getLogger(MindmapsEngineServer.class);

        // Set host name
        ipAddress(prop.getProperty(ConfigProperties.SERVER_HOST_NAME));

        // Set port
        port(prop.getPropertyAsInt(ConfigProperties.SERVER_PORT_NUMBER));

        // Set the external static files folder
        staticFiles.externalLocation(prop.getPath(ConfigProperties.STATIC_FILES_PATH));

        // Start all the controllers
        new RemoteShellController();
        new VisualiserController();
        new GraphFactoryController();
        new ImportController();
        new CommitLogController();
        new TransactionController();
        new StatusController();
        new BackgroundTasksController();

        //Register Exception Handler
        exception(MindmapsEngineServerException.class, (e, request, response) -> {
            response.status(((MindmapsEngineServerException) e).getStatus());
            response.body("New exception: "+e.getMessage()+" - Please refer to mindmaps.log file for full stack trace.");
        });

        // This method will block until all the controllers are ready to serve requests
        awaitInitialization();

        printStartMessage(prop.getProperty(ConfigProperties.SERVER_HOST_NAME), prop.getProperty(ConfigProperties.SERVER_PORT_NUMBER),prop.getLogFilePath());
    }

    public static void stop(){
        Spark.stop();
    }

    /**
     * Check if Mindmamps Engine has been started
     * @return true if Mindmaps Engine running, false otherwise
     */
    public static boolean isRunning(){
        try {
            String host = prop.getProperty(ConfigProperties.SERVER_HOST_NAME);
            String port = prop.getProperty(ConfigProperties.SERVER_PORT_NUMBER);

            HttpURLConnection connection = (HttpURLConnection)
                    new URL("http://" + host + ":" + port + REST.WebPath.GRAPH_FACTORY_URI).openConnection();
            connection.setRequestMethod("GET");
            connection.connect();

            InputStream inputStream = connection.getInputStream();
            if(inputStream.available() == 0){
                return false;
            }
        } catch (IOException e){
            return false;
        }
        return true;
    }

    /**
     * Method that prints a welcome message, listening address and path to the LOG that will be used.
     * @param host Host address to which Mindmaps Engine is bound to
     * @param port Web server port number
     * @param logFilePath Path to the LOG file.
     */
    private static void printStartMessage(String host, String port, String logFilePath) {
        String address = "http://" + host + ":" + port;
        LOG.info("\nGrakn LOG file located at ["+logFilePath+"]");
        LOG.info("\n==================================================");
        LOG.info("\n"+String.format(ConfigProperties.GRAKN_ASCII,address));
        LOG.info("\n==================================================");
    }
}
