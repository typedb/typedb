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


import ai.grakn.engine.backgroundtasks.distributed.DistributedTaskManager;
import ai.grakn.engine.postprocessing.PostProcessing;
import ai.grakn.engine.postprocessing.PostProcessingTask;
import ai.grakn.engine.backgroundtasks.distributed.ClusterManager;
import ai.grakn.engine.controller.AuthController;
import ai.grakn.engine.controller.TasksController;
import ai.grakn.engine.controller.UserController;
import ai.grakn.engine.controller.CommitLogController;
import ai.grakn.engine.controller.GraphFactoryController;
import ai.grakn.engine.controller.ImportController;
import ai.grakn.engine.controller.StatusController;
import ai.grakn.engine.controller.VisualiserController;
import ai.grakn.engine.session.RemoteSession;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.engine.util.JWTHandler;
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.util.REST;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Spark;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static spark.Spark.*;

/**
 * Main class in charge to start a web server and all the REST controllers.
 */

public class GraknEngineServer {
    private static ConfigProperties prop = ConfigProperties.getInstance();
    private static Logger LOG = LoggerFactory.getLogger(GraknEngineServer.class);
    private static final int WEBSOCKET_TIMEOUT = 3600000;
    private static final Set<String> unauthenticatedEndPoints = new HashSet<>(Arrays.asList(
            REST.WebPath.NEW_SESSION_URI,
            REST.WebPath.REMOTE_SHELL_URI,
            REST.WebPath.GRAPH_FACTORY_URI,
            REST.WebPath.IS_PASSWORD_PROTECTED_URI));

    public static final boolean isPasswordProtected = prop.getPropertyAsBool(ConfigProperties.PASSWORD_PROTECTED_PROPERTY);

    public static void main(String[] args) {
        startHTTP();
        startCluster();
        printStartMessage(prop.getProperty(ConfigProperties.SERVER_HOST_NAME), prop.getProperty(ConfigProperties.SERVER_PORT_NUMBER), prop.getLogFilePath());
    }

    public static void startHTTP() {
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
        new AuthController();
        new UserController();
        
        //Register filter to check authentication token in each request
        before(GraknEngineServer::checkAuthorization);

        //Register Exception Handler
        exception(GraknEngineServerException.class, (e, request, response) -> {
            response.status(((GraknEngineServerException) e).getStatus());
            response.body("New exception: " + e.getMessage() + " - Please refer to grakn.log file for full stack trace.");
        });

        // This method will block until all the controllers are ready to serve requests
        awaitInitialization();
    }

    public static void startCluster() {
        // Start background task cluster.
        ClusterManager.getInstance().start();

        // Submit a recurring post processing task
        //FIXME: other things open and close this too
        DistributedTaskManager manager = DistributedTaskManager.getInstance();
        manager.open();
        manager.scheduleTask(new PostProcessingTask(),
                             GraknEngineServer.class.getName(),
                             new Date(),
                             prop.getPropertyAsInt(ConfigProperties.TIME_LAPSE),
                             new JSONObject());

    }

    public static void stopHTTP() {
        Spark.stop();
    }

    public static void stopCluster() {
        DistributedTaskManager.getInstance().close();
        PostProcessing.getInstance().stop();
        ClusterManager.getInstance().stop();
    }

    /**
     * Check if Grakn Engine has been started
     *
     * @return true if Grakn Engine running, false otherwise
     */
    public static boolean isRunning() {
        try {
            String host = prop.getProperty(ConfigProperties.SERVER_HOST_NAME);
            String port = prop.getProperty(ConfigProperties.SERVER_PORT_NUMBER);

            HttpURLConnection connection = (HttpURLConnection)
                    new URL("http://" + host + ":" + port + REST.WebPath.GRAPH_FACTORY_URI).openConnection();
            connection.setRequestMethod("GET");
            connection.connect();

            InputStream inputStream = connection.getInputStream();
            if (inputStream.available() == 0) {
                return false;
            }
        } catch (IOException e) {
            return false;
        }
        return true;
    }


    private static void checkAuthorization(Request request, Response response) {
        if(!isPasswordProtected) return;

        //we dont check authorization token if the path requested is one of the unauthenticated ones
        if (!unauthenticatedEndPoints.contains(request.pathInfo())) {
            //add check to see if string contains substring "Bearer ", for now a lot of optimism here
            boolean authenticated;
            try {
                if (request.headers("Authorization") == null || !request.headers("Authorization").startsWith("Bearer "))
                    throw new GraknEngineServerException(400, "Authorization field in header corrupted or absent.");

                String token = request.headers("Authorization").substring(7);
                authenticated = JWTHandler.verifyJWT(token);
            } catch (Exception e) {
                //request is malformed, return 400
                throw new GraknEngineServerException(400, e);
            }
            if (!authenticated) {
                halt(401, "User not authenticated.");
            }
        }
    }


    /**
     * Method that prints a welcome message, listening address and path to the LOG that will be used.
     *
     * @param host        Host address to which Grakn Engine is bound to
     * @param port        Web server port number
     * @param logFilePath Path to the LOG file.
     */
    private static void printStartMessage(String host, String port, String logFilePath) {
        String address = "http://" + host + ":" + port;
        LOG.info("\nGrakn LOG file located at [" + logFilePath + "]");
        LOG.info("\n==================================================");
        LOG.info("\n" + String.format(ConfigProperties.GRAKN_ASCII, address));
        LOG.info("\n==================================================");
    }
}
