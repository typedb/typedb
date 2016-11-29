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


import ai.grakn.GraknGraph;
import ai.grakn.engine.controller.TasksController;
import ai.grakn.engine.controller.CommitLogController;
import ai.grakn.engine.controller.GraphFactoryController;
import ai.grakn.engine.controller.ImportController;
import ai.grakn.engine.controller.StatusController;
import ai.grakn.engine.controller.VisualiserController;
import ai.grakn.engine.session.RemoteSession;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.GraphFactory;
import ai.grakn.util.REST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;
import spark.utils.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import static spark.Spark.awaitInitialization;
import static spark.Spark.exception;
import static spark.Spark.ipAddress;
import static spark.Spark.port;
import static spark.Spark.staticFiles;
import static spark.Spark.webSocket;
import static spark.Spark.webSocketIdleTimeoutMillis;

/**
 * Main class in charge to start a web server and all the REST controllers.
 */

public class GraknEngineServer {

    private static ConfigProperties prop = ConfigProperties.getInstance();
    private static Logger LOG = LoggerFactory.getLogger(GraknEngineServer.class);
    private static final int WEBSOCKET_TIMEOUT = 3600000;

    public static void main(String[] args) {
        start();
    }

    public static void start() {

        // Set host name
        ipAddress(prop.getProperty(ConfigProperties.SERVER_HOST_NAME));

        // Set port
        port(prop.getPropertyAsInt(ConfigProperties.SERVER_PORT_NUMBER));

        // Set the external static files folder
        staticFiles.externalLocation(prop.getPath(ConfigProperties.STATIC_FILES_PATH));

        // Start the websocket for Graql
        webSocket(REST.WebPath.REMOTE_SHELL_URI, RemoteSession.class);
        webSocketIdleTimeoutMillis(WEBSOCKET_TIMEOUT);

        // Start all the controllers
        new VisualiserController();
        new GraphFactoryController();
        new ImportController();
        new CommitLogController();
        new StatusController();
        new TasksController();

        //Register Exception Handler
        exception(GraknEngineServerException.class, (e, request, response) -> {
            response.status(((GraknEngineServerException) e).getStatus());
            response.body("New exception: "+e.getMessage()+" - Please refer to grakn.log file for full stack trace.");
        });

        loadSystemOntology();

        // This method will block until all the controllers are ready to serve requests
        awaitInitialization();

        printStartMessage(prop.getProperty(ConfigProperties.SERVER_HOST_NAME), prop.getProperty(ConfigProperties.SERVER_PORT_NUMBER),prop.getLogFilePath());
    }

    public static void stop(){
        Spark.stop();
    }

    /**
     * Check if Grakn Engine has been started
     * @return true if Grakn Engine running, false otherwise
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
     * @param host Host address to which Grakn Engine is bound to
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

    private static void loadSystemOntology() {
        GraknGraph graph = GraphFactory.getInstance().getGraph(ConfigProperties.SYSTEM_GRAPH_NAME);
        ClassLoader loader = GraknEngineServer.class.getClassLoader();

        try {
            String query = IOUtils.toString(loader.getResourceAsStream(ConfigProperties.SYSTEM_ONTOLOGY_FILE));

            graph.graql()
                 .parse(query)
                 .execute();

            graph.commit();
        }
        catch(IOException | GraknValidationException | NullPointerException e) {
            LOG.error("Could not load system ontology. The error was: "+e);
        }
    }
}
