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

import ai.grakn.engine.controller.AuthController;
import ai.grakn.engine.controller.CommitLogController;
import ai.grakn.engine.controller.ConceptController;
import ai.grakn.engine.controller.DashboardController;
import ai.grakn.engine.controller.SystemController;
import ai.grakn.engine.controller.TasksController;
import ai.grakn.engine.controller.UserController;
import ai.grakn.engine.controller.GraqlController;
import ai.grakn.engine.postprocessing.PostProcessing;
import ai.grakn.engine.postprocessing.PostProcessingTask;
import ai.grakn.engine.postprocessing.UpdatingInstanceCountTask;
import ai.grakn.engine.session.RemoteSession;
import ai.grakn.engine.tasks.TaskManager;
import ai.grakn.engine.tasks.TaskSchedule;
import ai.grakn.engine.tasks.TaskState;
import ai.grakn.engine.util.EngineID;
import ai.grakn.engine.util.JWTHandler;
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.util.REST;
import mjson.Json;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static ai.grakn.engine.GraknEngineConfig.SERVER_HOST_NAME;
import static ai.grakn.engine.GraknEngineConfig.STATIC_FILES_PATH;
import static ai.grakn.engine.GraknEngineConfig.TASK_MANAGER_IMPLEMENTATION;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * Main class in charge to start a web server and all the REST controllers.
 *
 * @author Marco Scoppetta
 */

public class GraknEngineServer implements AutoCloseable {
    private static final GraknEngineConfig prop = GraknEngineConfig.getInstance();

    private static final Logger LOG = LoggerFactory.getLogger(GraknEngineServer.class);
    private static final int WEBSOCKET_TIMEOUT = 3600000;
    private static final Set<String> unauthenticatedEndPoints = new HashSet<>(Arrays.asList(
            REST.WebPath.NEW_SESSION_URI,
            REST.WebPath.REMOTE_SHELL_URI,
            REST.WebPath.System.CONFIGURATION,
            REST.WebPath.IS_PASSWORD_PROTECTED_URI));
    public static final boolean isPasswordProtected = prop.getPropertyAsBool(GraknEngineConfig.PASSWORD_PROTECTED_PROPERTY);

    private final EngineID engineId = EngineID.me();
    private final int port;
    private final Service spark = Service.ignite();
    private final TaskManager taskManager;

    private GraknEngineServer(String taskManagerClass, int port) {
        taskManager = startTaskManager(taskManagerClass);
        this.port = port;
        startHTTP();
        startRecurringBackgroundTasks();
        printStartMessage(prop.getProperty(SERVER_HOST_NAME), prop.getProperty(GraknEngineConfig.SERVER_PORT_NUMBER), prop.getLogFilePath());
    }

    public static void main(String[] args) {
        GraknEngineServer server = mainWithServer();

        // close GraknEngineServer on SIGTERM
        Thread closeThread = new Thread(server::close, "GraknEngineServer-shutdown");
        Runtime.getRuntime().addShutdownHook(closeThread);
    }

    public static GraknEngineServer mainWithServer() {
        // Start Engine
        int port = prop.getPropertyAsInt(GraknEngineConfig.SERVER_PORT_NUMBER);
        String taskManagerClass = prop.getProperty(TASK_MANAGER_IMPLEMENTATION);
        return start(taskManagerClass, port);
    }

    public static GraknEngineServer start(String taskManagerClass, int port){
        return new GraknEngineServer(taskManagerClass, port);
    }

    @Override
    public void close() {
        stopHTTP();
        stopTaskManager();
    }

    /**
     * Check in with the properties file to decide which type of task manager should be started
     */
    private TaskManager startTaskManager(String taskManagerClassName) {
        try {
            Class<TaskManager> taskManagerClass = (Class<TaskManager>) Class.forName(taskManagerClassName);
            return taskManagerClass.getConstructor(EngineID.class).newInstance(engineId);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalArgumentException("Invalid or unavailable TaskManager class", e);
        } catch (InvocationTargetException e) {
            throw (RuntimeException) e.getCause();
        }
    }

    public void startHTTP() {
        configureSpark(spark, port);

        // Start all the controllers
        EngineGraknGraphFactory factory = EngineGraknGraphFactory.getInstance();
        new GraqlController(factory, spark);
        new ConceptController(factory, spark);
        new DashboardController(factory, spark);
        new SystemController(spark);
        new CommitLogController(spark);
        new AuthController(spark);
        new UserController(spark);
        new TasksController(spark, taskManager);

        // This method will block until all the controllers are ready to serve requests
        spark.awaitInitialization();
    }


    public static void configureSpark(Service spark, int port){
        // Set host name
        spark.ipAddress(prop.getProperty(SERVER_HOST_NAME));

        // Set port
        spark.port(port);

        // Set the external static files folder
        spark.staticFiles.externalLocation(prop.getPath(STATIC_FILES_PATH));

        // Start the websocket for Graql
        spark.webSocket(REST.WebPath.REMOTE_SHELL_URI, RemoteSession.class);
        spark.webSocketIdleTimeoutMillis(WEBSOCKET_TIMEOUT);

        //Register filter to check authentication token in each request
        spark.before((req, res) -> checkAuthorization(spark, req));

        //Register exception handlers
        spark.exception(GraknEngineServerException.class, (e, req, res) -> handleGraknServerError(e, res));
        spark.exception(Exception.class,                  (e, req, res) -> handleInternalError(e, res));
    }

    private void startRecurringBackgroundTasks(){
        // Submit a recurring post processing task
        Duration interval = Duration.ofMillis(prop.getPropertyAsInt(GraknEngineConfig.TIME_LAPSE));
        String creator = GraknEngineServer.class.getName();

        TaskState postprocessing = TaskState.of(PostProcessingTask.class, creator, TaskSchedule.recurring(interval), Json.object());
        taskManager.addTask(postprocessing);

        TaskState updatingInstanceCount = TaskState.of(UpdatingInstanceCountTask.class, creator, TaskSchedule.recurring(interval), Json.object());
        taskManager.addTask(updatingInstanceCount);
    }

    public void stopHTTP() {
        spark.stop();

        // Block until server is truly stopped
        // This occurs when there is no longer a port assigned to the Spark server
        boolean running = true;
        while (running) {
            try {
                spark.port();
            }
            catch(IllegalStateException e){
                LOG.debug("Spark server has been stopped");
                running = false;
            }
        }
    }

    private void stopTaskManager() {
        PostProcessing.getInstance().stop();
        try {
            taskManager.close();
        } catch (Exception e){
            LOG.error(getFullStackTrace(e));
        }
    }

    public TaskManager getTaskManager(){
        return taskManager;
    }

    /**
     * If authorization is enabled, check the client has correct JWT Token before allowing
     * access to specific endpoints.
     * @param request request information from the client
     */
    private static void checkAuthorization(Service spark, Request request) {
        if(!isPasswordProtected) return;

        //we dont check authorization token if the path requested is one of the unauthenticated ones
        if (!unauthenticatedEndPoints.contains(request.pathInfo())) {
            //add check to see if string contains substring "Bearer ", for now a lot of optimism here
            boolean authenticated;
            try {
                if (request.headers("Authorization") == null || !request.headers("Authorization").startsWith("Bearer ")) {
                    throw new GraknEngineServerException(400, "Authorization field in header corrupted or absent.");
                }

                String token = request.headers("Authorization").substring(7);
                authenticated = JWTHandler.verifyJWT(token);
            } catch (Exception e) {
                //request is malformed, return 400
                throw new GraknEngineServerException(400, e);
            }
            if (!authenticated) {
                spark.halt(401, "User not authenticated.");
            }
        }
    }

    /**
     * Handle any {@link GraknEngineServerException} that are thrown by the server. Configures and returns
     * the correct JSON response.
     *
     * @param exception exception thrown by the server
     * @param response response to the client
     */
    private static void handleGraknServerError(Exception exception, Response response){
        response.status(((GraknEngineServerException) exception).getStatus());
        response.body(Json.object("exception", exception.getMessage()).toString());
        response.type(ContentType.APPLICATION_JSON.getMimeType());
    }

    /**
     * Handle any exception thrown by the server
     * @param exception Exception by the server
     * @param response response to the client
     */
    private static void handleInternalError(Exception exception, Response response){
        response.status(500);
        response.body(Json.object("exception", exception.getMessage()).toString());
        response.type(ContentType.APPLICATION_JSON.getMimeType());
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
        LOG.info("\n" + String.format(GraknEngineConfig.GRAKN_ASCII, address));
        LOG.info("\n==================================================");
    }
}
