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
package ai.grakn.engine;


import ai.grakn.engine.controller.CommitLogController;
import ai.grakn.engine.controller.GraphFactoryController;
import ai.grakn.engine.controller.ImportController;
import ai.grakn.engine.controller.RemoteShellController;
import ai.grakn.engine.controller.StatusController;
import ai.grakn.engine.controller.TransactionController;
import ai.grakn.engine.controller.VisualiserController;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.exception.GraknEngineServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

import static spark.Spark.*;

/**
 * Main class in charge to start a web server and all the REST controllers.
 */

public class GraknEngineServer {

    private static Logger LOG = null;


    public static void main(String[] args) {
        start();
    }

    public static void start() {

        ConfigProperties prop = ConfigProperties.getInstance();

        LOG = LoggerFactory.getLogger(GraknEngineServer.class);

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


        //Register Exception Handler
        exception(GraknEngineServerException.class, (e, request, response) -> {
            response.status(((GraknEngineServerException) e).getStatus());
            response.body("New exception: "+e.getMessage()+" - Please refer to grakn.log file for full stack trace.");
        });

        // This method will block until all the controllers are ready to serve requests
        awaitInitialization();

        printStartMessage(prop.getProperty(ConfigProperties.SERVER_HOST_NAME), prop.getProperty(ConfigProperties.SERVER_PORT_NUMBER),prop.getLogFilePath());
    }

    public static void stop(){
        Spark.stop();
    }


    /**
     * Method that prints a welcome message, listening address and path to the LOG that will be used.
     * @param host Host address to which Grakn Engine is bound to
     * @param port Web server port number
     * @param logFilePath Path to the LOG file.
     */
    private static void printStartMessage(String host, String port, String logFilePath) {
        LOG.info(ConfigProperties.GRAKN_ASCII);
        LOG.info("Grakn Engine is ready. Listening on [http://" + host + ":" + port + "]");
        LOG.info("Grakn LOG file located at ["+logFilePath+"]");
    }
}
